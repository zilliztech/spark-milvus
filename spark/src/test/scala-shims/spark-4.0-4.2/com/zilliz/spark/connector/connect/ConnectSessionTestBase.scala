package com.zilliz.spark.connector.connect

import java.io.File
import java.net.ServerSocket
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import java.util.Comparator

import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.spark.connector.extensions.MilvusSparkPlugin
import com.zilliz.spark.connector.testkit.LocalMilvusCollection

/** Owns a fixed-file Spark Connect server and a separate client JVM. The
  * concrete suite selects its production extension and client program.
  */
abstract class ConnectSessionTestBase
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  protected val line =
    org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")
  protected val clientClasspath =
    Option(System.getProperty("milvus.test.connect.client.classpath"))
      .filter(_.nonEmpty)

  protected var available = true
  private var directory: Path = _
  protected var options: Map[String, String] = Map.empty
  private var port = 0
  protected var spark: SparkSession = _

  protected def sessionExtensions: String

  override def beforeAll(): Unit = {
    try {
      NativeStorageLibrary.load()
      NativeVectorLibrary.load()
    } catch {
      case _: UnsatisfiedLinkError | _: NoClassDefFoundError |
          _: RuntimeException =>
        available = false
    }
    if (available && clientClasspath.nonEmpty) {
      directory = Files.createTempDirectory("milvus-connect-")
      options = LocalMilvusCollection.load("api", directory)
      port = {
        val socket = new ServerSocket(0)
        try socket.getLocalPort
        finally socket.close()
      }
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("milvus-connect")
        .config(
          "spark.sql.extensions",
          sessionExtensions
        )
        .config("spark.plugins", classOf[MilvusSparkPlugin].getName)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.connect.grpc.binding.address", "127.0.0.1")
        .config("spark.connect.grpc.binding.port", port.toString)
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      SparkConnectService.start(spark.sparkContext)
    }
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      SparkConnectService.stop()
      spark.stop()
    }
    if (directory != null) {
      val paths = Files.walk(directory)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }

  /** Runs the client program and returns its lines by their first field. */
  protected def client(program: String): Map[String, Seq[Seq[String]]] = {
    val output = Files.createTempFile("milvus-connect-client-", ".log")
    try {
      val java = new File(System.getProperty("java.home"), "bin/java").getPath
      val command = Seq(
        java,
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "-cp",
        clientClasspath.get,
        program,
        s"sc://127.0.0.1:$port",
        line
      ) ++ options.toSeq.sorted.map { case (key, value) => s"$key=$value" }
      val process = new ProcessBuilder(command: _*)
        .redirectErrorStream(true)
        .redirectOutput(output.toFile)
        .start()
      val finished = process.waitFor(5, TimeUnit.MINUTES)
      if (!finished) process.destroyForcibly()
      val printed = new String(Files.readAllBytes(output), UTF_8)
      withClue(s"client output:\n$printed\n") {
        finished shouldBe true
        process.exitValue() shouldBe 0
      }
      printed.linesIterator
        .map(_.split('\t').toSeq)
        .filter(fields =>
          Set(
            "built",
            "written",
            "filtered",
            "method",
            "sql",
            "views",
            "rows",
            "repeat"
          )
            .contains(fields.head)
        )
        .toSeq
        .groupBy(_.head)
        .map { case (key, lines) => key -> lines.map(_.tail) }
    } finally Files.deleteIfExists(output)
  }

}
