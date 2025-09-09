package com.zilliz.spark.connector.jni

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success, Try}

object MilvusStorageJNI {
  // Declare native methods for milvus-storage operations
  @native def createSpace(storagePath: String, schemaJson: String): Long
  @native def writeData(spaceHandle: Long, dataJson: String): Boolean
  @native def readData(spaceHandle: Long, columnNames: Array[String]): String
  @native def readDataWithFilter(
      spaceHandle: Long,
      columnNames: Array[String],
      filterJson: String
  ): String
  @native def getSpaceVersion(spaceHandle: Long): Long
  @native def getStorageSize(storagePath: String): Long
  @native def closeSpace(spaceHandle: Long): Boolean
  @native def deleteSpace(storagePath: String): Boolean

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
    // Check if we're in a JAR environment by looking for the resource
    val jniLibrary = "libmilvus_storage_jni.so"
    val jniResourcePath = s"/native/$jniLibrary"
    val jniInputStream =
      findResourceStream(jniResourcePath, currentClassLoader)

    if (jniInputStream.isEmpty) {
      throw new RuntimeException(
        s"JNI library $jniLibrary not found in resources"
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

  // High-level wrapper methods
  case class MilvusSpace(handle: Long, storagePath: String) {
    def write(dataJson: String): Try[Boolean] = {
      Try(writeData(handle, dataJson))
    }

    def read(columnNames: Array[String]): Try[String] = {
      Try(readData(handle, columnNames))
    }

    def readWithFilter(
        columnNames: Array[String],
        filterJson: String
    ): Try[String] = {
      Try(readDataWithFilter(handle, columnNames, filterJson))
    }

    def getVersion: Try[Long] = {
      Try(getSpaceVersion(handle))
    }

    def close(): Try[Boolean] = {
      Try(closeSpace(handle))
    }
  }

  def openSpace(storagePath: String, schemaJson: String): Try[MilvusSpace] = {
    Try {
      ensureLibraryLoaded()
      val handle = createSpace(storagePath, schemaJson)
      if (handle == 0) {
        throw new RuntimeException("Failed to create/open milvus space")
      }
      MilvusSpace(handle, storagePath)
    }
  }

  def getStorageSizeWrapper(storagePath: String): Try[Long] = {
    Try(getStorageSize(storagePath))
  }

  def deleteStorage(storagePath: String): Try[Boolean] = {
    Try(deleteSpace(storagePath))
  }

  // Test method to verify JNI is working properly
  def testMilvusStorage(): Unit = {
    try {
      println("Testing Milvus Storage JNI library...")

      val tempPath =
        java.nio.file.Files.createTempDirectory("milvus_test").toString
      println(s"Using temporary storage path: $tempPath")

      // Create a simple schema
      val schemaJson = """
      {
        "fields": [
          {"name": "id", "type": "int64", "primary": true},
          {"name": "timestamp", "type": "int64", "version": true},
          {"name": "vector", "type": "fixed_size_binary", "size": 128},
          {"name": "label", "type": "string"}
        ]
      }
      """

      // Test space creation
      openSpace(tempPath, schemaJson) match {
        case Success(space) =>
          println("Space created successfully!")

          // Test data writing
          val testData = """
          {
            "batches": [
              {
                "id": [1, 2, 3],
                "timestamp": [1000, 1001, 1002],
                "vector": ["vector1", "vector2", "vector3"],
                "label": ["label1", "label2", "label3"]
              }
            ]
          }
          """

          space.write(testData) match {
            case Success(true) =>
              println("Data written successfully!")

              // Test data reading
              space.read(Array("id", "label")) match {
                case Success(result) =>
                  println(s"Data read successfully: $result")
                case Failure(e) =>
                  println(s"Failed to read data: ${e.getMessage}")
              }

            case Success(false) =>
              println("Failed to write data")
            case Failure(e) =>
              println(s"Error writing data: ${e.getMessage}")
          }

          // Close space
          space.close() match {
            case Success(true) => println("Space closed successfully")
            case _             => println("Failed to close space")
          }

        case Failure(e) =>
          println(s"Failed to create space: ${e.getMessage}")
      }

      // Clean up
      deleteStorage(tempPath) match {
        case Success(true) => println("Storage cleaned up successfully")
        case _             => println("Failed to clean up storage")
      }

      println("Milvus Storage JNI library test completed!")
    } catch {
      case e: Exception =>
        println(s"Milvus Storage JNI library test failed: ${e.getMessage}")
        throw e
    }
  }

  // Main method for standalone testing
  def main(args: Array[String]): Unit = {
    println("=== Standalone Milvus Storage JNI Test ===")
    testMilvusStorage()
    println("=== Test completed ===")
  }
}
