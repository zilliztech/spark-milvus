package com.zilliz.spark.connector.jni

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success, Try}

object MilvusStorageJNI {
  // ==================== Reader C API Native Methods ====================

  // ReadProperties methods
  @native def readPropertiesDefault(): Long
  @native def readPropertiesCreate(keys: Array[String], values: Array[String]): Long
  @native def readPropertiesGet(properties: Long, key: String): String
  @native def readPropertiesFree(properties: Long): Unit

  // ChunkReader methods
  @native def getChunkIndices(reader: Long, rowIndices: Array[Long]): Array[Long]
  @native def getChunk(reader: Long, chunkIndex: Long): Long
  @native def getChunks(reader: Long, chunkIndices: Array[Long], parallelism: Long): Array[Long]
  @native def chunkReaderDestroy(reader: Long): Unit

  // Reader methods
  @native def readerNew(                            
      fs: Long,
      manifest: String,
      schema: Long,
      neededColumns: Array[String],
      properties: Long
  ): Long
  @native def getRecordBatchReader(
      reader: Long,
      predicate: String,
      batchSize: Long,
      bufferSize: Long
  ): Long
  @native def getChunkReader(reader: Long, columnGroupId: Long): Long
  @native def take(reader: Long, rowIndices: Array[Long], parallelism: Long): Long
  @native def readerDestroy(reader: Long): Unit

  // Arrow helper methods
  @native def releaseArrowArray(array: Long): Unit
  @native def releaseArrowSchema(schema: Long): Unit
  @native def releaseArrowArrayStream(stream: Long): Unit
  @native def getArrowArrayLength(array: Long): Long
  @native def getArrowArrayNumChildren(array: Long): Long

  // Use AtomicBoolean for thread-safe library loading in Spark environment
  private val libraryLoaded = new AtomicBoolean(false)
  private val loadLock = new Object()

  private def ensureLibraryLoaded(): Unit = {
    if (!libraryLoaded.get()) {
      loadLock.synchronized {
        if (!libraryLoaded.get()) {
          try {
            // Get current thread's class loader to handle Spark's complex class loading
            val currentClassLoader =
              Thread.currentThread().getContextClassLoader

            // Load from resources (handles JAR environment and extracts to persistent location)
            loadFromResources(currentClassLoader)
            // System.loadLibrary("milvus-storage")
            // System.loadLibrary("milvus_storage_jni")
            libraryLoaded.set(true)
          } catch {
            case e: Exception =>
              println(s"Failed to load native library: ${e.getMessage}")
              println(
                s"Current ClassLoader: ${Thread.currentThread().getContextClassLoader}"
              )
              println(s"Class ClassLoader: ${getClass.getClassLoader}")
              throw new RuntimeException("Could not load native library", e)
          }
        }
      }
    }
  }

  private def loadFromResources(currentClassLoader: ClassLoader): Unit = {
    // Try to load the reader JNI library first, then fall back to storage JNI
    val libraries = List("libmilvus_storage_jni.so")

    var jniLibrary: String = null
    var jniInputStream: Option[java.io.InputStream] = None

    for (lib <- libraries if jniInputStream.isEmpty) {
      val resourcePath = s"/native/$lib"
      jniInputStream = findResourceStream(resourcePath, currentClassLoader)
      if (jniInputStream.isDefined) {
        jniLibrary = lib
        println(s"Found JNI library: $jniLibrary")
      }
    }

    if (jniInputStream.isEmpty) {
      throw new RuntimeException(
        s"No JNI library found in resources. Tried: ${libraries.mkString(", ")}"
      )
    }

    // Create a persistent directory for all extracted libraries
    val userHome = Option(System.getProperty("user.home")).getOrElse("/tmp")
    val persistentDir = new java.io.File(userHome, ".milvus-native-libs")
    if (!persistentDir.exists()) {
      persistentDir.mkdirs()
    }

    // Extract JNI library to persistent location
    val jniTempFile =
      extractLibraryToPersistentDir(
        jniInputStream.get,
        jniLibrary,
        persistentDir
      )

    // Check if LD_PRELOAD is set with our dependency library
    val ldPreload = Option(System.getenv("LD_PRELOAD")).getOrElse("")
    val hasMilvusStoragePreloaded = ldPreload.contains("libmilvus-storage.so")

    if (!hasMilvusStoragePreloaded) {
      // Check if dependency library exists in resources and extract it
      val depLibrary = "libmilvus-storage.so"
      val depResourcePath = s"/native/$depLibrary"
      val depInputStream =
        findResourceStream(depResourcePath, currentClassLoader)

      if (depInputStream.isDefined) {
        // Extract dependency library to the same persistent directory
        val depTempFile = extractLibraryToPersistentDir(
          depInputStream.get,
          depLibrary,
          persistentDir
        )
        println(
          s"Extracted dependency library to: ${depTempFile.getAbsolutePath}"
        )

        // Try to preload the dependency library first
        try {
          System.load(depTempFile.getAbsolutePath)
          println(s"Successfully preloaded dependency: $depLibrary")
        } catch {
          case e: UnsatisfiedLinkError
              if e.getMessage.contains("static TLS block") =>
            throw new RuntimeException(
              s"Cannot load dependency library due to TLS memory allocation issue. " +
                s"Please preload the library using LD_PRELOAD environment variable before starting the JVM. " +
                s"Example: LD_PRELOAD=${depTempFile.getAbsolutePath} java ... " +
                s"Current LD_PRELOAD: '$ldPreload' " +
                s"Original error: ${e.getMessage}",
              e
            )
          case e: Exception =>
            println(
              s"Warning: Failed to preload dependency library: ${e.getMessage}"
            )
          // Continue anyway, maybe the system has the library
        }
      } else {
        println(
          s"Warning: Dependency library $depLibrary not found in resources. Assuming it's available in system."
        )
      }
    } else {
      println(
        s"Detected LD_PRELOAD with libmilvus-storage.so, skipping dependency library extraction"
      )
    }

    // Load the main JNI library
    try {
      System.load(jniTempFile.getAbsolutePath)
      println(
        s"Successfully loaded $jniLibrary from resources: ${jniTempFile.getAbsolutePath}"
      )
    } catch {
      case e: UnsatisfiedLinkError
          if e.getMessage.contains("static TLS block") =>
        // Get the path to the extracted dependency library for error message
        val depLibPath = new java.io.File(
          persistentDir,
          "libmilvus-storage.so"
        ).getAbsolutePath
        throw new RuntimeException(
          s"Failed to load JNI library due to TLS memory allocation issue. " +
            s"This typically happens when the dependency library (libmilvus-storage.so) is not preloaded. " +
            s"Please use LD_PRELOAD to preload the dependency library before starting the JVM. " +
            s"Example: LD_PRELOAD=$depLibPath java ... " +
            s"Current LD_PRELOAD: '$ldPreload' " +
            s"Original error: ${e.getMessage}",
          e
        )
      case e: Exception =>
        throw new RuntimeException(
          s"Failed to load JNI library: ${e.getMessage}",
          e
        )
    }
  }

  private def findResourceStream(
      resourcePath: String,
      currentClassLoader: ClassLoader
  ): Option[java.io.InputStream] = {
    // Try multiple class loaders to find the resource
    Option(getClass.getResourceAsStream(resourcePath))
      .orElse(
        Option(currentClassLoader).flatMap(cl =>
          Option(cl.getResourceAsStream(resourcePath))
        )
      )
      .orElse(
        Option(ClassLoader.getSystemResourceAsStream(resourcePath))
      )
  }

  private def extractLibraryToPersistentDir(
      inputStream: java.io.InputStream,
      libName: String,
      persistentDir: java.io.File
  ): java.io.File = {
    val libFile = new java.io.File(persistentDir, libName)

    // Only extract if file doesn't exist or is older
    if (!libFile.exists() || shouldUpdateLibrary(inputStream, libFile)) {
      val outputStream = new java.io.FileOutputStream(libFile)
      try {
        val buffer =
          new Array[Byte](8192) // Larger buffer for better performance
        var bytesRead = inputStream.read(buffer)
        while (bytesRead != -1) {
          outputStream.write(buffer, 0, bytesRead)
          bytesRead = inputStream.read(buffer)
        }
      } finally {
        inputStream.close()
        outputStream.close()
      }

      // Set executable permissions
      libFile.setExecutable(true)
      libFile.setReadable(true)

      println(
        s"Extracted $libName to persistent location: ${libFile.getAbsolutePath}"
      )
    } else {
      inputStream.close() // Close the input stream even if we don't use it
      println(
        s"Using existing $libName from persistent location: ${libFile.getAbsolutePath}"
      )
    }

    libFile
  }

  private def shouldUpdateLibrary(
      inputStream: java.io.InputStream,
      existingFile: java.io.File
  ): Boolean = {
    // For simplicity, always use existing file if it exists
    // In a more sophisticated implementation, you could compare checksums or timestamps
    true
  }

  // Reader C API wrapper classes

  /**
   * Wrapper for ReadProperties
   */
  class ReadProperties private(val ptr: Long) extends AutoCloseable {
    def get(key: String): Option[String] = {
      Option(readPropertiesGet(ptr, key))
    }

    override def close(): Unit = {
      if (ptr != 0) readPropertiesFree(ptr)
    }
  }

  object ReadProperties {
    def apply(): ReadProperties = {
      ensureLibraryLoaded()
      new ReadProperties(readPropertiesDefault())
    }

    def apply(properties: Map[String, String]): ReadProperties = {
      ensureLibraryLoaded()
      val keys = properties.keys.toArray
      val values = properties.values.toArray
      val ptr = readPropertiesCreate(keys, values)
      if (ptr == 0) throw new RuntimeException("Failed to create read properties")
      new ReadProperties(ptr)
    }
  }

  /**
   * Wrapper for ChunkReader
   */
  class ChunkReader private[MilvusStorageJNI](val ptr: Long) extends AutoCloseable {
    def getChunkIndices(rowIndices: Array[Long]): Try[Array[Long]] = Try {
      val result = MilvusStorageJNI.getChunkIndices(ptr, rowIndices)
      if (result == null) throw new RuntimeException("Failed to get chunk indices")
      result
    }

    def getChunk(chunkIndex: Long): Try[Long] = Try {
      val result = MilvusStorageJNI.getChunk(ptr, chunkIndex)
      if (result == 0) throw new RuntimeException("Failed to get chunk")
      result
    }

    def getChunks(chunkIndices: Array[Long], parallelism: Long = 1): Try[Array[Long]] = Try {
      val result = MilvusStorageJNI.getChunks(ptr, chunkIndices, parallelism)
      if (result == null) throw new RuntimeException("Failed to get chunks")
      result
    }

    override def close(): Unit = {
      if (ptr != 0) chunkReaderDestroy(ptr)
    }
  }

  /**
   * Wrapper for RecordBatchReader
   */
  class RecordBatchReader private[MilvusStorageJNI](val ptr: Long) extends AutoCloseable {
    override def close(): Unit = {
      if (ptr != 0) releaseArrowArrayStream(ptr)
    }
  }

  /**
   * Main Reader wrapper
   */
  class Reader private(val ptr: Long) extends AutoCloseable {
    def getRecordBatchReader(
        predicate: Option[String] = None,
        batchSize: Long = 10000,
        bufferSize: Long = 1024 * 1024
    ): Try[RecordBatchReader] = Try {
      val predicateStr = predicate.orNull
      val streamPtr = MilvusStorageJNI.getRecordBatchReader(ptr, predicateStr, batchSize, bufferSize)
      if (streamPtr == 0) throw new RuntimeException("Failed to create record batch reader")
      new RecordBatchReader(streamPtr)
    }

    def getChunkReader(columnGroupId: Long): Try[ChunkReader] = Try {
      val chunkReaderPtr = MilvusStorageJNI.getChunkReader(ptr, columnGroupId)
      if (chunkReaderPtr == 0) throw new RuntimeException("Failed to create chunk reader")
      new ChunkReader(chunkReaderPtr)
    }

    def take(rowIndices: Array[Long], parallelism: Long = 1): Try[Long] = Try {
      val result = MilvusStorageJNI.take(ptr, rowIndices, parallelism)
      if (result == 0) throw new RuntimeException("Failed to take rows")
      result
    }

    override def close(): Unit = {
      if (ptr != 0) readerDestroy(ptr)
    }
  }

  object Reader {
    def apply(
        fs: Long,
        manifest: String,
        schema: Long,
        neededColumns: Option[Array[String]] = None,
        properties: Option[ReadProperties] = None
    ): Reader = {
      ensureLibraryLoaded()
      val columnsArray = neededColumns.orNull
      val propertiesPtr = properties.map(_.ptr).getOrElse(0L)
      val readerPtr = readerNew(fs, manifest, schema, columnsArray, propertiesPtr)
      if (readerPtr == 0) throw new RuntimeException("Failed to create reader")
      new Reader(readerPtr)
    }
  }

  /**
   * Arrow array utilities
   */
  object ArrowUtils {
    def getArrayLength(arrayPtr: Long): Long = getArrowArrayLength(arrayPtr)
    def getArrayNumChildren(arrayPtr: Long): Long = getArrowArrayNumChildren(arrayPtr)
    def releaseArray(arrayPtr: Long): Unit = releaseArrowArray(arrayPtr)
    def releaseSchema(schemaPtr: Long): Unit = releaseArrowSchema(schemaPtr)
    def releaseStream(streamPtr: Long): Unit = releaseArrowArrayStream(streamPtr)
  }

  // Test method for Reader API
  def testReaderAPI(): Unit = {
    try {
      println("Testing Milvus Storage Reader API...")
      ensureLibraryLoaded()

      // Test ReadProperties
      println("Testing ReadProperties...")
      val props = ReadProperties(Map(
        "batch_size" -> "5000",
        "buffer_size" -> "1048576"
      ))

      props.get("batch_size") match {
        case Some(value) => println(s"  ReadProperties.get('batch_size'): $value")
        case None => println("  ReadProperties.get('batch_size'): not found")
      }

      // Test with dummy pointers (would need real filesystem and schema in production)
      println("\nTesting Reader creation with dummy values...")
      try {
        // This will fail with dummy values but tests that the native methods are accessible
        val reader = Reader(
          fs = 0L, // Would be real filesystem handle
          manifest = "/dummy/manifest.json",
          schema = 0L, // Would be real Arrow schema
          neededColumns = Some(Array("id", "vector", "metadata")),
          properties = Some(props)
        )
        println("  Reader created (unexpected with dummy values)")
        reader.close()
      } catch {
        case e: RuntimeException if e.getMessage.contains("Failed to create reader") =>
          println("  Reader creation failed as expected with dummy values")
        case e: UnsatisfiedLinkError =>
          println(s"  Native library not fully loaded: ${e.getMessage}")
      }

      // Test Arrow utilities
      println("\nTesting Arrow utilities...")
      val invalidLength = ArrowUtils.getArrayLength(0L)
      println(s"  ArrowUtils.getArrayLength(0): $invalidLength")

      props.close()
      println("\nReader API test completed!")

    } catch {
      case e: Exception =>
        println(s"Reader API test failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  // Main method for standalone testing
  def main(args: Array[String]): Unit = {
    println("=== Standalone Milvus Storage JNI Test ===")

    testReaderAPI()

    println("\n=== All tests completed ===")
  }
}
