package com.zilliz.spark.connector.read

import java.net.URI
import java.util.Locale

import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.procedure.{
  BuildIndexProcedure,
  ProcedureArgs,
  WriteSnapshotProcedure
}

/** Builds a vector index over every segment of a snapshot with the
  * `build_index` procedure and writes the snapshot that carries it with
  * `write_snapshot`, as a Spark job on a cluster. The last line printed is the
  * key of the new snapshot JSON, which an APPROX NEAREST BY reads when it is
  * the base's `milvus.snapshot.path`.
  *
  * {{{
  *   --snapshot <location>      required: the source snapshot JSON
  *   --collection <db.name>     required: the collection the snapshot describes
  *   --output <prefix>          required: bucket-relative prefix for the index
  *                              objects and the new snapshot; same bucket as
  *                              the data
  *   --field emb                --index-type HNSW  --metric L2
  *   --params M=16,efConstruction=200
  *   --restorable false         true requires --output to be the instance root
  *   --option key=value         repeatable; extra connector options; a value
  *                              `env:NAME` is read from the environment
  *   --bucket --root-path --region --endpoint
  *                              storage location; derived from --snapshot when
  *                              it names a bucket
  * }}}
  */
object IndexBuildJob {

  private val Repeatable = Set("option")

  final case class Arguments(
      single: Map[String, String],
      repeated: Map[String, Seq[String]]
  ) {
    def get(name: String): Option[String] = single.get(name)
    def apply(name: String, fallback: => String): String =
      single.getOrElse(name, fallback)
    def pairs(name: String): Map[String, String] =
      repeated
        .getOrElse(name, Seq.empty)
        .map { entry =>
          val at = entry.indexOf('=')
          require(at > 0, s"--$name takes key=value, not '$entry'")
          entry.substring(0, at) -> resolve(entry.substring(at + 1))
        }
        .toMap

    /** `env:NAME` takes the value from the environment, so a secret can reach
      * an option through a Kubernetes secretKeyRef instead of the command line.
      */
    private def resolve(value: String): String =
      if (value.startsWith("env:")) {
        val name = value.stripPrefix("env:")
        sys.env.getOrElse(
          name,
          throw new IllegalArgumentException(
            s"environment variable $name is not set"
          )
        )
      } else value
  }

  def parse(args: Array[String]): Arguments = {
    require(
      args.length % 2 == 0,
      s"Arguments come in --name value pairs: ${args.mkString(" ")}"
    )
    val pairs = args
      .grouped(2)
      .map { case Array(name, value) =>
        require(name.startsWith("--"), s"'$name' is not an option name")
        name.stripPrefix("--") -> value
      }
      .toSeq
    val repeated = pairs
      .filter { case (name, _) => Repeatable(name) }
      .groupBy(_._1)
      .map { case (name, values) => name -> values.map(_._2) }
    val single = pairs.filterNot { case (name, _) => Repeatable(name) }
    val duplicated = single.groupBy(_._1).collect {
      case (name, values) if values.size > 1 => name
    }
    require(
      duplicated.isEmpty,
      s"Given more than once: ${duplicated.mkString(", ")}"
    )
    Arguments(single.toMap, repeated)
  }

  /** Where a snapshot location says the base lives: bucket, the instance root
    * (the path before `snapshots/`) and, for the https form, the endpoint and
    * region.
    */
  final case class Location(
      bucket: Option[String],
      rootPath: Option[String],
      endpoint: Option[String],
      region: Option[String]
  )

  def locate(snapshot: String): Location = {
    val uri = new URI(snapshot)
    val segments =
      Option(uri.getPath).getOrElse("").split('/').filter(_.nonEmpty)
    def rootOf(keys: Seq[String]): Option[String] = {
      val at = keys.indexOf("snapshots")
      if (at > 0) Some(keys.take(at).mkString("/")) else None
    }
    Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)) match {
      case Some("https") | Some("http") =>
        val host = uri.getHost
        val region = "^s3[.-]([a-z0-9-]+)\\.amazonaws\\.com$".r
          .findFirstMatchIn(host)
          .map(_.group(1))
        Location(
          segments.headOption,
          rootOf(segments.drop(1).toSeq),
          Some(host),
          region
        )
      case Some("s3") | Some("s3a") =>
        Location(Option(uri.getAuthority), rootOf(segments.toSeq), None, None)
      case _ => Location(None, None, None, None)
    }
  }

  def main(args: Array[String]): Unit = {
    val arguments = parse(args)
    def required(name: String): String = arguments
      .get(name)
      .getOrElse(throw new IllegalArgumentException(s"--$name is required"))
    val snapshot = required("snapshot")
    val collection = required("collection")
    val output = required("output")
    val field = arguments("field", "emb")
    val indexType = arguments("index-type", "HNSW").toUpperCase(Locale.ROOT)
    val metric = arguments("metric", "L2").toUpperCase(Locale.ROOT)
    val params = arguments("params", "M=16,efConstruction=200")
    val restorable = arguments("restorable", "false").toBoolean

    val location = locate(snapshot)
    val region = arguments("region", location.region.getOrElse("us-west-2"))
    val bucket = arguments
      .get("bucket")
      .orElse(location.bucket)
      .getOrElse(throw new IllegalArgumentException("--bucket is required"))
    val rootPath = arguments.get("root-path").orElse(location.rootPath)
    val endpoint = arguments(
      "endpoint",
      location.endpoint.getOrElse(s"s3.$region.amazonaws.com")
    )
    val options = Map(
      StorageProperties.BucketName -> bucket,
      StorageProperties.Address -> endpoint,
      StorageProperties.Region -> region,
      StorageProperties.CloudProvider -> "aws",
      StorageProperties.UseSSL -> "true",
      StorageProperties.UseIam -> "true",
      MilvusOption.SnapshotPath -> snapshot
    ) ++ rootPath.map(StorageProperties.RootPath -> _) ++
      arguments.pairs("option")

    val builder = SparkSession.builder().appName("IndexBuildJob")
    arguments.get("master").foreach(builder.master)
    val spark = builder.getOrCreate()
    val buildId = System.currentTimeMillis()
    report(
      s"plan collection=$collection field=$field index_type=$indexType " +
        s"metric=$metric params=$params output=$output restorable=$restorable " +
        s"snapshot=$snapshot bucket=$bucket root=${rootPath.getOrElse("")}"
    )

    val buildStarted = System.nanoTime()
    val built = BuildIndexProcedure.run(
      ProcedureArgs(
        values = Map(
          "collection" -> collection,
          "field" -> field,
          "output" -> output,
          "index_type" -> indexType,
          "metric" -> metric,
          "params" -> params,
          "build_id" -> buildId
        ),
        options = options
      )
    )
    val buildSeconds = (System.nanoTime() - buildStarted) / 1e9
    val rows = built.map(_.getLong(2)).sum
    val objects = built.map(_.getInt(3)).sum
    val bytes = built.map(_.getLong(4)).sum
    val job = built.head.getString(6)
    report(
      f"RESULT build segments=${built.size} rows=$rows objects=$objects " +
        f"index_bytes=$bytes build_seconds=$buildSeconds%.1f " +
        f"rows_per_second=${rows / buildSeconds}%.0f job=$job"
    )

    val writeStarted = System.nanoTime()
    val written = WriteSnapshotProcedure.run(
      ProcedureArgs(
        values = Map(
          "collection" -> collection,
          "job" -> job,
          "input" -> output,
          "snapshot_id" -> buildId,
          "snapshot_name" -> s"$collection-$indexType-$buildId",
          "restorable" -> restorable
        ),
        options = options
      )
    )
    val key = written.head.getString(0)
    report(
      f"RESULT write_snapshot key=$key segments=${written.head.getInt(3)} " +
        f"indexes=${written.head.getInt(4)} bytes=${written.head.getLong(5)} " +
        f"write_seconds=${(System.nanoTime() - writeStarted) / 1e9}%.1f"
    )
    report(s"SNAPSHOT $key")
    spark.stop()
  }

  private def report(line: String): Unit =
    println(s"[IndexBuildJob] $line")
}
