package com.zilliz.spark.connector.connect

import java.io.File
import java.net.ServerSocket
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import java.util.Comparator
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.spark.connector.extensions.{
  MilvusSparkPlugin,
  MilvusSparkSessionExtensions
}
import com.zilliz.spark.connector.implicits._
import com.zilliz.spark.connector.testkit.LocalMilvusCollection
import com.zilliz.spark.connector.testkit.LocalMilvusCollection.{
  Entity,
  Segment
}

/** The connector's DataFrame methods and SQL from a Spark Connect client, on
  * the 4.x lines (docs/design/architecture/dataframe-api.html sections 6 and
  * 9).
  *
  * The server runs here, in the test JVM, over a classic session with the
  * connector's extension and plugin. The client runs in a JVM of its own
  * ([[ConnectClientProgram]]): the Connect client jar carries the sql-api
  * classes rebuilt against a relocated Arrow, which would replace the classic
  * ones in this JVM. Its classpath is the client jar and this connector's
  * classes, which the build passes in `milvus.test.connect.client.classpath`,
  * so the client also shows that `MilvusDataFrame` loads without classic Spark.
  */
class ConnectSessionTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private val line =
    org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")
  private val clientClasspath =
    Option(System.getProperty("milvus.test.connect.client.classpath"))
      .filter(_.nonEmpty)

  private var available = true
  private var directory: Path = _
  private var options: Map[String, String] = Map.empty
  private var port = 0
  private var spark: SparkSession = _

  private val segments = Seq(30L, 31L).map(segment =>
    Segment(
      segment,
      (0 until 8).map(row =>
        Entity(
          segment * 100 + row,
          Array(row.toFloat + 1f, (segment - 29).toFloat),
          Some(row % 2L)
        )
      )
    )
  )

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
      options = LocalMilvusCollection.write(directory, 2, segments)
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
          classOf[MilvusSparkSessionExtensions].getName
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
  private def client(): Map[String, Seq[Seq[String]]] = {
    val output = Files.createTempFile("milvus-connect-client-", ".log")
    try {
      val java = new File(System.getProperty("java.home"), "bin/java").getPath
      val command = Seq(
        java,
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "-cp",
        clientClasspath.get,
        ConnectClientProgram.getClass.getName.stripSuffix("$"),
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
          Set("built", "written", "filtered", "method", "sql", "views")
            .contains(fields.head)
        )
        .toSeq
        .groupBy(_.head)
        .map { case (key, lines) => key -> lines.map(_.tail) }
    } finally Files.deleteIfExists(output)
  }

  test("a Connect client builds, delivers and searches as a classic one does") {
    assume(available, "the native libraries are not on this machine")
    assume(
      clientClasspath.nonEmpty,
      "the build passes the Connect client's classpath in milvus.test.connect.client.classpath"
    )
    val printed = client()

    val classic = spark.read
      .format("milvus")
      .options(options)
      .load()
      .buildIndex("vector", "built/classic", metric = "IP", buildId = 7101L)
      .collect()
      .map(row =>
        Seq(
          row.getAs[Long]("segment_id"),
          row.getAs[Long]("row_count"),
          row.getAs[Long]("build_id")
        ).map(_.toString)
      )
      .toSeq
      .sortBy(_.head)
    printed("built") shouldBe classic
    printed("written").map(_.tail) shouldBe Seq(Seq("2", "2"))
    printed("filtered").head.head should include("its plan holds Filter")

    val nearest = Seq(Seq("q1", "3105"), Seq("q1", "3106"), Seq("q1", "3107"))
    if (line == "4.2") printed("method") shouldBe nearest
    else
      printed("method").head.head should include(
        "nearestByJoin runs on a classic Spark session; on Spark Connect write " +
          "it in SQL as nearest_by_join"
      )
    printed("sql") shouldBe nearest
    printed("views") shouldBe Seq(Seq("0"))
  }
}
