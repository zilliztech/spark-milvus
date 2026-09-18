package com.zilliz.spark.connector.procedure

import java.util.Locale
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.milvus.storage.write.commit.{CommittedIndex, Committer}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{MilvusOption, SnapshotReference}
import com.zilliz.spark.connector.read.SnapshotPartitions
import com.zilliz.spark.connector.table.MilvusTables
import io.milvus.grpc.schema.FieldSchema

/** `CALL milvus.system.build_index(...)`: builds a vector index for every
  * segment of a fixed snapshot and writes the objects under a prefix the call
  * names.
  *
  * The job is planned like a read — one task per segment of the snapshot the
  * options select — and each task builds what section 2.4's loader can read
  * back. The records land in the job manifest, which is what delivery (W8)
  * turns into a snapshot Milvus can restore
  * (docs/design/architecture/vector-search.html section 2.7).
  */
object BuildIndexProcedure extends Procedure {

  override val name: String = "build_index"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("field", StringType),
    Parameter("output", StringType),
    Parameter("index_type", StringType, required = false),
    Parameter("metric", StringType, required = false),
    Parameter("params", StringType, required = false),
    Parameter("build_id", LongType, required = false),
    Parameter("index_version", LongType, required = false),
    Parameter("store_path_version", LongType, required = false)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("segment_id", LongType, nullable = false),
      StructField("partition_id", LongType, nullable = false),
      StructField("row_count", LongType, nullable = false),
      StructField("objects", IntegerType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("build_id", LongType, nullable = false),
      StructField("job_id", StringType, nullable = false)
    )
  )

  override def run(args: ProcedureArgs): Seq[Row] = {
    val (database, collection) =
      ProcedureSupport.parseCollection(args.string("collection"), name)
    val options = ProcedureSupport.collectionOptions(
      args.options,
      database,
      collection,
      name
    )
    val output = args.string("output").trim.stripSuffix("/")
    require(
      output.nonEmpty && !output.contains("://"),
      s"'output' is a prefix inside the bucket, not a URI: '$output'"
    )
    val caseInsensitive = new CaseInsensitiveStringMap(options.asJava)
    val table = MilvusTables.load(
      caseInsensitive,
      None,
      SnapshotReference.Configured
    )
    val column = args.string("field")
    val field = table.snapshot.schema.fields
      .find(_.name == column)
      .getOrElse(
        throw new IllegalArgumentException(
          s"The collection has no vector field '$column'"
        )
      )
    val partitions = SnapshotPartitions.of(table, caseInsensitive)
    require(
      partitions.nonEmpty,
      s"The snapshot of '$collection' holds no segment to index"
    )
    val jobId = "index-" + System.currentTimeMillis()
    val spec = SegmentIndexBuild.Spec(
      vectorColumn = column,
      layout = VectorLayout.of(field.dataType, dimensionOf(field)),
      fieldId = field.fieldID,
      collectionId = table.snapshot.collectionId,
      indexType = args
        .stringOpt("index_type")
        .map(_.toUpperCase(Locale.ROOT))
        .getOrElse("HNSW"),
      metric = args
        .stringOpt("metric")
        .map(_.toUpperCase(Locale.ROOT))
        .getOrElse("COSINE"),
      parameters = parametersOf(args.stringOpt("params")),
      buildId = args.longOpt("build_id").getOrElse(System.currentTimeMillis()),
      indexVersion = args.longOpt("index_version").getOrElse(1L),
      storePathVersion = args.longOpt("store_path_version").getOrElse(0L).toInt,
      output = output,
      arrowMaxBytes = MilvusOption(caseInsensitive).readLimits.arrowMaxBytes
    )
    require(spec.buildId > 0, s"'build_id' must be positive: ${spec.buildId}")
    require(
      spec.indexVersion > 0,
      s"'index_version' must be positive: ${spec.indexVersion}"
    )

    val spark = SparkSession.active
    // Segments go to tasks in the order they were planned, so a task's segments
    // are a contiguous range of the plan and no two tasks share one.
    val indexes = spark.sparkContext
      .parallelize(partitions, partitions.size)
      .map(partition => SegmentIndexBuild.run(partition, spec))
      .collect()
      .toSeq

    // The manifest goes to the bucket the tasks wrote to, through the same
    // properties the planner bound to this snapshot.
    commit(indexes, partitions.head.task.properties, output, jobId)

    indexes.map { index =>
      Row(
        index.segmentId,
        index.partitionId,
        index.rowCount,
        index.filePaths.size,
        index.serializedSize,
        index.buildId,
        jobId
      )
    }
  }

  /** Writes the records where a delivery job reads them: one job manifest under
    * the output prefix.
    */
  private def commit(
      indexes: Seq[CommittedIndex],
      properties: Map[String, String],
      output: String,
      jobId: String
  ): Unit = {
    val store = NativeObjectStore.Factory(properties).open()
    try
      new Committer(store, StagingLayout(output, jobId))
        .commit(Seq.empty, indexes = indexes)
    finally store.close()
  }

  private def dimensionOf(field: FieldSchema): Int = field.typeParams
    .find(_.key == "dim")
    .map(_.value.toInt)
    .getOrElse(
      throw new IllegalArgumentException(
        s"Field '${field.name}' declares no dimension"
      )
    )

  /** `params => 'M=16,efConstruction=200'`: what the caller tunes, which the
    * Spark layer parses and `core.index` hands to Knowhere.
    */
  private[procedure] def parametersOf(
      params: Option[String]
  ): Map[String, String] = params
    .map(_.trim)
    .filter(_.nonEmpty)
    .map { text =>
      text
        .split(',')
        .map(_.trim)
        .filter(_.nonEmpty)
        .map { pair =>
          val parts = pair.split('=')
          require(
            parts.length == 2 && parts(0).trim.nonEmpty && parts(
              1
            ).trim.nonEmpty,
            s"'params' takes name=value pairs separated by commas, not '$pair'"
          )
          parts(0).trim -> parts(1).trim
        }
        .toMap
    }
    .getOrElse(Map.empty)
}
