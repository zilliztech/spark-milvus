package com.zilliz.spark.connector.procedure

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.codec.{IndexObjectTarget, SegmentIndexObjects}
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.read.exec.{RowExclusions, SegmentVectors}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.milvus.storage.vector.IndexBuilder
import com.zilliz.milvus.storage.write.commit.CommittedIndex
import com.zilliz.spark.connector.read.{ColumnBinding, MilvusInputPartition}
import com.zilliz.spark.connector.types.ArrowAllocator
import io.milvus.grpc.schema.CollectionSchema

/** Builds one segment's vector index in one task.
  *
  * The task reads the segment's vector column into one buffer, Knowhere builds
  * and serializes, and the Milvus `TableFormat` side writes the objects. What
  * comes back is the record a segment's index entry is written from
  * (docs/design/projects/spark-milvus/architecture/vector-search.html section
  * 2.7).
  */
private[procedure] object SegmentIndexBuild extends Logging {

  /** What every task of one build needs and the driver already knows. */
  final case class Request(
      vectorColumn: String,
      layout: VectorLayout,
      fieldId: Long,
      collectionId: Long,
      indexType: String,
      metric: MetricType,
      parameters: Map[String, String],
      buildId: Long,
      indexVersion: Long,
      storePathVersion: Int,
      output: String,
      arrowMaxBytes: Long
  ) extends Serializable

  def run(partition: MilvusInputPartition, request: Request): CommittedIndex = {
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
      request.arrowMaxBytes
    )
    try {
      val vectors = SegmentVectors.open(
        task,
        binding.arrowSchema,
        binding.columnNameFor,
        binding.arrowColumnFor(request.vectorColumn),
        request.layout,
        exclusions,
        allocator.allocator
      )
      val assembled =
        try
          IndexBuilder.assemble(
            vectors,
            request.layout,
            segmentRows,
            allocator.allocator
          )
        finally vectors.close()
      try {
        val built = IndexBuilder.buildFromAddress(
          assembled.address,
          assembled.byteLength,
          assembled.rows,
          request.layout,
          request.indexType,
          request.metric,
          vectorIndexVersion,
          request.parameters,
          assembled.validData
        )
        try write(task, request, built, vectorIndexVersion)
        finally built.close()
      } finally assembled.close()
    } finally allocator.close()
  }

  /** Writes the payloads as index objects and says what the segment record will
    * carry.
    */
  private def write(
      task: SegmentReadTask,
      request: Request,
      built: IndexBuilder.Built,
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
      request.collectionId,
      task.partitionId,
      task.segmentId,
      request.fieldId,
      request.buildId,
      request.indexVersion,
      request.storePathVersion,
      built.validData.nonEmpty,
      request.output
    )
    val store = NativeObjectStore.Factory(task.properties).open()
    val objects =
      try SegmentIndexObjects.write(payloads, read, target, store)
      finally store.close()
    logInfo(
      s"Segment index built: segment=${task.segmentId}, type=${request.indexType}, " +
        s"metric=${request.metric}, rows=${built.rows}, objects=${objects.size}, " +
        s"bytes=${objects.map(_.bytes).sum}"
    )
    CommittedIndex(
      segmentId = task.segmentId,
      partitionId = task.partitionId,
      fieldId = request.fieldId,
      buildId = request.buildId,
      indexVersion = request.indexVersion,
      vectorIndexVersion = vectorIndexVersion,
      storePathVersion = request.storePathVersion,
      indexType = request.indexType,
      metricType = request.metric.name,
      rowCount = built.rows,
      serializedSize = objects.map(_.bytes).sum,
      filePaths = objects.map(_.key),
      params = request.parameters
    )
  }
}
