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

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.milvus.storage.write.commit.{
  CommittedIndex,
  Committer,
  SnapshotWriter,
  SourceSnapshot
}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{
  MilvusOption,
  OptionParsing,
  SnapshotReference,
  TaskResources
}
import com.zilliz.spark.connector.read.SnapshotPartitions
import com.zilliz.spark.connector.table.MilvusTables
import io.milvus.grpc.schema.FieldSchema

/** `CALL milvus.system.build_index(...)`: builds a vector index for every
  * segment of a fixed snapshot and writes the objects under a prefix the call
  * names.
  *
  * The table is `collection`, read with the options the call gives, or `table`,
  * a name Spark resolves to a whole Milvus table, read with the snapshot and
  * options that read pinned; `MilvusDataFrame.buildIndex` passes a temporary
  * view of its DataFrame. The job is planned like a read — one task per segment
  * of the snapshot the read selects — and each task builds what section 2.4's
  * loader can read back. The records land in the job manifest together with the
  * snapshot the job planned against, which is what delivery (W8) copies into a
  * snapshot Milvus can restore (docs/design/architecture/vector-search.html
  * section 2.7, docs/design/architecture/dataframe-api.html section 9). A
  * snapshot not read from a snapshot document cannot be delivered, so it is
  * refused before the job runs.
  */
object BuildIndexProcedure extends Procedure {

  override val name: String = "build_index"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType, required = false),
    Parameter("field", StringType),
    Parameter("output", StringType),
    Parameter("index_type", StringType, required = false),
    Parameter("metric", StringType, required = false),
    Parameter("params", StringType, required = false),
    Parameter("build_id", LongType, required = false),
    Parameter("index_version", LongType, required = false),
    Parameter("store_path_version", LongType, required = false),
    Parameter("table", StringType, required = false)
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
    val tableName = ProcedureSupport.tableName(args, name)
    val collection = tableName.getOrElse(args.string("collection"))
    val output = args.string("output").trim.stripSuffix("/")
    require(
      output.nonEmpty && !output.contains("://"),
      s"'output' is a prefix inside the bucket, not a URI: '$output'"
    )
    val spark = SparkSession.active
    val read = tableName match {
      case Some(table) =>
        val read = MilvusTables.named(spark, table)
        ProcedureSupport.rejectFilter(read.options, name)
        read
      case None =>
        val (database, collectionName) =
          ProcedureSupport.parseCollection(collection, name)
        val options = new CaseInsensitiveStringMap(
          ProcedureSupport
            .collectionOptions(args.options, database, collectionName, name)
            .asJava
        )
        ProcedureSupport.rejectFilter(options, name)
        MilvusTables.Read(
          MilvusTables.load(options, None, SnapshotReference.Configured),
          options
        )
    }
    val table = read.table
    val caseInsensitive = read.options
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
    // What write_snapshot will copy. A snapshot that was not read from a
    // snapshot document can never be written, so the build stops here, before
    // the job runs (docs/design/architecture/dataframe-api.html section 9).
    val source = SnapshotWriter
      .sourceOf(
        table.snapshot,
        partitions.head.task.properties
          .getOrElse(StorageProperties.Address, "")
      )
      .getOrElse(
        throw new IllegalArgumentException(
          s"'$collection' was not read from a snapshot document, so no snapshot can be written from this build: " +
            s"${table.snapshot.origin}"
        )
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
        .map(name =>
          MetricType
            .fromName(name)
            .getOrElse(
              throw new IllegalArgumentException(
                s"'metric' must be one of ${MetricType.values.mkString(", ")}: $name"
              )
            )
        )
        .getOrElse(MetricType.Cosine),
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

    // Segments go to tasks in the order they were planned, so a task's segments
    // are a contiguous range of the plan and no two tasks share one. Knowhere
    // builds on a thread pool the size of the machine, so a build task takes
    // every core of its executor (docs/design/architecture/vector-search.html
    // section 1.1).
    val built = spark.sparkContext
      .parallelize(partitions, partitions.size)
      .map(partition => SegmentIndexBuild.run(partition, spec))
    val indexes = TaskResources
      .of(spark)
      .wholeExecutor
      .fold(built)(built.withResources)
      .collect()
      .toSeq

    // The manifest goes to the bucket the tasks wrote to, through the same
    // properties the planner bound to this snapshot.
    commit(indexes, partitions.head.task.properties, output, jobId, source)

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
    * the output prefix, naming the snapshot the indexes were built over.
    */
  private def commit(
      indexes: Seq[CommittedIndex],
      properties: Map[String, String],
      output: String,
      jobId: String,
      source: SourceSnapshot
  ): Unit = {
    val store = NativeObjectStore.Factory(properties).open()
    try
      new Committer(store, StagingLayout(output, jobId))
        .commit(Seq.empty, indexes = indexes, sourceSnapshot = Some(source))
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
  ): Map[String, String] =
    params.map(OptionParsing.namedValues(_, "'params'")).getOrElse(Map.empty)
}
