package com.zilliz.spark.connector.procedure

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.codec.{IndexObjectTarget, SegmentIndexObjects}
import com.zilliz.milvus.storage.index.IndexWriter
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.read.exec.{RowExclusions, SegmentVectors}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.milvus.storage.write.commit.CommittedIndex
import com.zilliz.spark.connector.read.{ColumnBinding, MilvusInputPartition}
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

/** Builds one segment's vector index in one task.
  *
  * The task reads the segment's vector column into one buffer, Knowhere builds
  * and serializes, and the Milvus `TableFormat` side writes the objects. What
  * comes back is the record a segment's index entry is written from
  * (docs/design/architecture/vector-search.html section 2.7).
  */
private[procedure] object SegmentIndexBuild extends Logging {

  /** What every task of one build needs and the driver already knows. */
  final case class Spec(
      vectorColumn: String,
      layout: VectorLayout,
      fieldId: Long,
      collectionId: Long,
      indexType: String,
      metric: String,
      parameters: Map[String, String],
      buildId: Long,
      indexVersion: Long,
      storePathVersion: Int,
      output: String,
      arrowMaxBytes: Long
  ) extends Serializable

  def run(partition: MilvusInputPartition, spec: Spec): CommittedIndex = {
    val task = partition.task
    val segmentRows = task.expectedRows.getOrElse(
      throw new IllegalArgumentException(
        s"Segment ${task.segmentId} does not say how many rows it holds"
      )
    )
    val binding = ColumnBinding(partition, StructType(Seq.empty))
    val collection = CollectionSchema.parseFrom(task.schemaBytes)
    // An index covers the segment as it was written: deletes are applied when a
    // search runs, not here, and only a null vector has nothing to index.
    val exclusions = RowExclusions.of(
      task.copy(deletes = DeleteSource.None),
      collection,
      None,
      binding.columnNameFor
    )
    val vectorIndexVersion = NativeVectorLibrary.load().currentIndexVersion()
    val allocator = ArrowAllocator.forIndexBuild(
      task.segmentId,
      spec.arrowMaxBytes
    )
    try {
      val vectors = SegmentVectors.open(
        task,
        binding.arrowSchema,
        binding.columnNameFor,
        binding.arrowColumnFor(spec.vectorColumn),
        spec.layout,
        exclusions,
        allocator.allocator
      )
      val assembled =
        try
          IndexWriter.assemble(
            vectors,
            spec.layout,
            segmentRows,
            allocator.allocator
          )
        finally vectors.close()
      try {
        val built = IndexWriter.build(
          assembled.bytes,
          assembled.rows,
          spec.layout,
          spec.indexType,
          spec.metric,
          vectorIndexVersion,
          spec.parameters,
          assembled.validData
        )
        try write(task, spec, built, vectorIndexVersion)
        finally built.close()
      } finally assembled.close()
    } finally allocator.close()
  }

  /** Writes the payloads as index objects and says what the segment record will
    * carry.
    */
  private def write(
      task: SegmentReadTask,
      spec: Spec,
      built: IndexWriter.Built,
      vectorIndexVersion: Int
  ): CommittedIndex = {
    val payloads = built.names.map(name => name -> built.length(name)) ++
      built.validData.map(bitmap => "valid_data" -> bitmap.length.toLong)
    def read(name: String, offset: Long, destination: ByteBuffer): Unit =
      built.validData match {
        case Some(bitmap) if name == "valid_data" =>
          destination.put(bitmap, offset.toInt, destination.remaining())
        case _ => built.read(name, offset, destination)
      }
    val target = IndexObjectTarget(
      spec.collectionId,
      task.partitionId,
      task.segmentId,
      spec.fieldId,
      spec.buildId,
      spec.indexVersion,
      spec.storePathVersion,
      built.validData.nonEmpty,
      spec.output
    )
    val store = NativeObjectStore.Factory(task.properties).open()
    val objects =
      try SegmentIndexObjects.write(payloads, read, target, store)
      finally store.close()
    logInfo(
      s"Segment index built: segment=${task.segmentId}, type=${spec.indexType}, " +
        s"metric=${spec.metric}, rows=${built.rows}, objects=${objects.size}, " +
        s"bytes=${objects.map(_.bytes).sum}"
    )
    CommittedIndex(
      segmentId = task.segmentId,
      partitionId = task.partitionId,
      fieldId = spec.fieldId,
      buildId = spec.buildId,
      indexVersion = spec.indexVersion,
      vectorIndexVersion = vectorIndexVersion,
      storePathVersion = spec.storePathVersion,
      indexType = spec.indexType,
      metricType = spec.metric,
      rowCount = built.rows,
      serializedSize = objects.map(_.bytes).sum,
      filePaths = objects.map(_.key),
      params = spec.parameters
    )
  }
}
