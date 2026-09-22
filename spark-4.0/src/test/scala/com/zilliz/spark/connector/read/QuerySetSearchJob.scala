package com.zilliz.spark.connector.read

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType}

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.spark.connector.options.MilvusOption

/** Searches a query set against a Milvus snapshot on object storage, writes
  * every query's top k as parquet, and scores the result against a ground-truth
  * file. On a cluster the master comes from the submission; `--master local[n]`
  * runs the same job in one JVM.
  *
  * The connector reads the base straight from the segment files the snapshot
  * lists; no Milvus service takes part. Each output row is one hit: `query_id`,
  * `rank`, the collection's primary key as `id`, `score`, `_segment_id` and
  * `_row_offset`. With a neighbors file the job also reports recall@k: for each
  * query, the share of the first k ground-truth ids found in its k hits,
  * averaged over the queries of the ground truth.
  *
  * {{{
  *   --snapshot <location>        required: the snapshot JSON, e.g. the
  *                                s3_location describe_snapshot prints
  *   --mode exact|index           default exact
  *   --master local[8]            optional; unset on a cluster
  *   --queries <path>             required: parquet with an id and a vector column
  *   --repeat 1                   the query set this many times over; copy c's
  *                                ids are shifted by c × (largest id + 1), so
  *                                every copy of a query has to return the
  *                                same hits as the query itself
  *   --query-id-column id         --query-vector-column emb
  *   --vector-field emb           --pk-field id
  *   --k 10                       --metric COSINE
  *   --output <prefix>            required: results go to <prefix>/<application id>-<mode>
  *   --neighbors <path>|none      default none: ground truth for recall@k
  *   --neighbors-column neighbors_id
  *   --search-param key=value     repeatable; index-mode parameters such as ef
  *   --option key=value           repeatable; extra connector options; a
  *                                value `env:NAME` is read from the environment
  *   --bucket --root-path --region --endpoint
  *                                storage location; derived from --snapshot when
  *                                it names a bucket
  * }}}
  *
  * The storage identity is the process's: the connector gets `fs.use_iam=true`
  * and uses the native default credential chain, which takes the pod's web
  * identity token first. Spark's own reads and writes (queries, neighbors,
  * output) go through s3a and use what the submission sets under
  * `spark.hadoop.fs.s3a.*`. An `s3://` path is read as `s3a://`.
  */
object QuerySetSearchJob {

  private val Repeatable = Set("search-param", "option")

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

  def sparkPath(path: String): String =
    if (path.startsWith("s3://")) "s3a://" + path.stripPrefix("s3://")
    else path

  def main(args: Array[String]): Unit = {
    val arguments = parse(args)
    val snapshot = arguments
      .get("snapshot")
      .getOrElse(throw new IllegalArgumentException("--snapshot is required"))
    val mode = arguments("mode", "exact").toLowerCase(Locale.ROOT)
    require(
      mode == "exact" || mode == "index",
      s"--mode is '$mode'; it takes exact or index"
    )
    val k = arguments("k", "10").toInt
    val metric = arguments("metric", "COSINE")
    val vectorField = arguments("vector-field", "emb")
    val pkField = arguments("pk-field", "id")
    val queriesPath = sparkPath(
      arguments
        .get("queries")
        .getOrElse(throw new IllegalArgumentException("--queries is required"))
    )
    val neighborsPath = arguments("neighbors", "none")
    val location = locate(snapshot)
    val region = arguments("region", location.region.getOrElse("us-west-2"))
    val bucket = arguments
      .get("bucket")
      .orElse(location.bucket)
      .getOrElse(
        throw new IllegalArgumentException(
          s"--bucket is required: '$snapshot' names no bucket"
        )
      )
    val rootPath = arguments.get("root-path").orElse(location.rootPath)
    val endpoint = arguments(
      "endpoint",
      location.endpoint.getOrElse(s"s3.$region.amazonaws.com")
    )

    val builder = SparkSession.builder().appName("QuerySetSearchJob")
    arguments.get("master").foreach(builder.master)
    val spark = builder.getOrCreate()
    val applicationId = spark.sparkContext.applicationId
    val output =
      sparkPath(
        arguments
          .get("output")
          .map(_.stripSuffix("/") + s"/$applicationId-$mode")
          .getOrElse(throw new IllegalArgumentException("--output is required"))
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

    val loaded = spark.read
      .parquet(queriesPath)
      .select(
        col(arguments("query-id-column", "id")).cast(LongType).as("query_id"),
        col(arguments("query-vector-column", "emb"))
          .cast(ArrayType(FloatType))
          .as("vector")
      )
    val repeat = arguments("repeat", "1").toInt
    require(repeat > 0, s"--repeat is $repeat; it takes a positive count")
    val queries =
      if (repeat == 1) loaded
      else {
        val span = loaded.agg(max(col("query_id"))).head().getLong(0) + 1L
        require(span > 0L, s"--repeat needs non-negative query ids")
        (0 until repeat)
          .map(copy =>
            loaded.withColumn("query_id", col("query_id") + lit(copy * span))
          )
          .reduce(_ union _)
      }
    val queryCount = queries.count()
    report(
      s"plan mode=$mode k=$k metric=$metric queries=$queryCount from " +
        s"$queriesPath x $repeat; snapshot=$snapshot bucket=$bucket " +
        s"root=${rootPath.getOrElse("")} output=$output"
    )

    val started = System.nanoTime()
    val hits = MilvusSearch.search(
      spark,
      options,
      queries,
      vectorField,
      k,
      metric,
      mode = mode,
      searchParameters = arguments.pairs("search-param"),
      outputColumns = Seq(pkField)
    )
    hits
      .select(
        col("query_id"),
        col("rank"),
        col(pkField).cast(LongType).as("id"),
        col("_score").as("score"),
        col("_segment_id"),
        col("_row_offset")
      )
      .write
      .mode("errorifexists")
      .parquet(output)
    val searchSeconds = (System.nanoTime() - started) / 1e9

    val written = spark.read.parquet(output)
    val hitCount = written.count()
    val answered = written.select("query_id").distinct().count()
    val recall =
      if (neighborsPath.equalsIgnoreCase("none")) None
      else
        Some(
          recallAt(
            spark,
            written,
            sparkPath(neighborsPath),
            arguments("neighbors-column", "neighbors_id"),
            k
          )
        )

    val summary = Seq(
      "application_id" -> applicationId,
      "mode" -> mode,
      "k" -> k.toString,
      "metric" -> metric,
      "queries" -> queryCount.toString,
      "answered_queries" -> answered.toString,
      "hits" -> hitCount.toString,
      "search_seconds" -> f"$searchSeconds%.3f",
      "queries_per_second" -> f"${queryCount / searchSeconds}%.1f",
      s"recall_at_$k" -> recall.map(r => f"$r%.4f").getOrElse("not computed"),
      "snapshot" -> snapshot,
      "queries_path" -> queriesPath,
      "output" -> output
    )
    report(
      "RESULT " + summary
        .map { case (key, value) => s"$key=$value" }
        .mkString(" ")
    )
    writeSummary(spark, output, summary)
    spark.stop()
  }

  /** Mean over the ground truth's queries of |hits ∩ first k truth ids| / k. A
    * query with no hits scores 0.
    */
  def recallAt(
      spark: SparkSession,
      hits: DataFrame,
      neighborsPath: String,
      neighborsColumn: String,
      k: Int
  ): Double = {
    val truth = spark.read
      .parquet(neighborsPath)
      .select(
        col("id").cast(LongType).as("query_id"),
        slice(col(neighborsColumn).cast(ArrayType(LongType)), 1, k).as("truth")
      )
    val found = hits
      .groupBy("query_id")
      .agg(collect_list(col("id")).as("found"))
    truth
      .join(found, Seq("query_id"), "left")
      .select(
        (coalesce(
          size(array_intersect(col("truth"), col("found"))),
          lit(0)
        ).cast("double") / lit(k.toDouble)).as("recall")
      )
      .agg(avg(col("recall")))
      .head()
      .getDouble(0)
  }

  /** `_SUMMARY.json` next to the parquet files; readers skip names that start
    * with an underscore.
    */
  private def writeSummary(
      spark: SparkSession,
      output: String,
      summary: Seq[(String, String)]
  ): Unit = {
    val path = new Path(output, "_SUMMARY.json")
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val json = summary
      .map { case (key, value) =>
        "  \"" + key + "\": \"" + value
          .replace("\\", "\\\\")
          .replace("\"", "\\\"") + "\""
      }
      .mkString("{\n", ",\n", "\n}\n")
    val stream = fs.create(path, false)
    try stream.write(json.getBytes(StandardCharsets.UTF_8))
    finally stream.close()
  }

  private def report(line: String): Unit =
    println(s"[QuerySetSearchJob] $line")
}
