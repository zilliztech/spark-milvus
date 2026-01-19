package com.zilliz.spark.connector.read

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.MilvusS3Option
import io.milvus.grpc.schema.CollectionSchema

// Unified PartitionReaderFactory that dispatches to V1 or V2 readers based on partition type
class MilvusPartitionReaderFactory(
    schema: StructType,
    optionsMap: Map[String, String],  // Use Map instead of CaseInsensitiveStringMap for serialization
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReaderFactory with Logging {

  // Reconstruct CaseInsensitiveStringMap for V1 reader
  private val readerOptions = {
    import scala.jdk.CollectionConverters._
    import java.util.HashMap
    val javaMap = new HashMap[String, String]()
    optionsMap.foreach { case (k, v) => javaMap.put(k, v) }
    MilvusS3Option(new CaseInsensitiveStringMap(javaMap))
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case milvusPartition: MilvusInputPartition =>
        logInfo(s"Creating V1 reader for partition with segmentID=${milvusPartition.segmentID}")
        // Create the V1 data reader with the file map, schema, and options
        new MilvusPartitionReader(
          schema,
          milvusPartition.fieldFiles,
          milvusPartition.partition,
          readerOptions,
          pushedFilters,
          milvusPartition.segmentID
        )

      case p: MilvusStorageV2InputPartition =>
        logInfo(s"Creating V2 reader for partition")

        // Storage V2 doesn't support system fields (row_id, timestamp) and extra metadata columns (segment_id, row_offset)
        // Filter them out from the schema for the underlying reader
        val v2Schema = StructType(schema.fields.filter { field =>
          field.name != "row_id" &&
          field.name != "timestamp" &&
          field.name != "segment_id" &&
          field.name != "row_offset"
        })

        // Deserialize the protobuf schema
        val milvusSchema = CollectionSchema.parseFrom(p.milvusSchemaBytes)

        // Create MilvusLoonPartitionReader directly
        val underlyingReader = new MilvusLoonPartitionReader(
          v2Schema,
          p.manifestJson,
          milvusSchema,
          p.milvusOption,
          optionsMap,
          p.topK,
          p.queryVector,
          p.metricType,
          p.vectorColumn,
          pushedFilters
        )

        // If the expected schema includes system/metadata fields, wrap the reader to add them
        val hasRowId = schema.fieldNames.contains("row_id")
        val hasTimestamp = schema.fieldNames.contains("timestamp")
        val hasSegmentId = schema.fieldNames.contains("segment_id")
        val hasRowOffset = schema.fieldNames.contains("row_offset")

        if (hasRowId || hasTimestamp || hasSegmentId || hasRowOffset) {
          new PartitionReader[InternalRow] {
            private var rowOffset: Long = 0L

            override def next(): Boolean = underlyingReader.next()

            override def get(): InternalRow = {
              val row = underlyingReader.get()

              // Build result row with system/metadata fields
              val numFields = schema.fields.length
              val resultValues = new Array[Any](numFields)

              var writeIdx = 0
              var readIdx = 0

              // Add system fields with null values
              if (hasRowId) {
                resultValues(writeIdx) = null
                writeIdx += 1
              }
              if (hasTimestamp) {
                resultValues(writeIdx) = null
                writeIdx += 1
              }

              // Copy actual data from underlying reader
              while (readIdx < v2Schema.fields.length) {
                val value = row.get(readIdx, v2Schema.fields(readIdx).dataType)
                resultValues(writeIdx) = value
                readIdx += 1
                writeIdx += 1
              }

              // Add metadata fields (segment_id and row_offset)
              if (hasSegmentId) {
                resultValues(writeIdx) = p.segmentID
                writeIdx += 1
              }
              if (hasRowOffset) {
                resultValues(writeIdx) = rowOffset
                rowOffset += 1
                writeIdx += 1
              }

              InternalRow.fromSeq(resultValues.toSeq)
            }

            override def close(): Unit = underlyingReader.close()
          }
        } else {
          underlyingReader
        }

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported partition type: ${partition.getClass.getName}"
        )
    }
  }
}
