package com.zilliz.spark.connector.read

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import com.zilliz.spark.connector.MilvusOption
import io.milvus.grpc.schema.CollectionSchema

object MilvusPartitionReaderFactory {
  private[read] def requestedExtraColumns(
      optionsMap: Map[String, String]
  ): Set[String] = {
    optionsMap
      .collectFirst {
        case (key, value)
            if key.equalsIgnoreCase(MilvusOption.MilvusExtraColumns) =>
          value
      }
      .toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(MilvusOption.normalizeExtraColumnName)
      .toSet
  }

  private[read] def isMetadataExtraField(
      name: String,
      requestedExtraColumns: Set[String]
  ): Boolean =
    requestedExtraColumns.contains(name)

  private[read] def stringValue(value: String): UTF8String =
    UTF8String.fromString(value)
}

// PartitionReaderFactory for Storage V2 (Milvus 2.6+)
class MilvusPartitionReaderFactory(
    schema: StructType,
    optionsMap: Map[String, String],
    pushedFilters: Array[Filter] = Array.empty[Filter],
    packedV2DeleteContext: MilvusPackedV2DeleteContext =
      MilvusPackedV2DeleteContext.empty
) extends PartitionReaderFactory
    with Logging {

  private val requestedExtraColumns =
    MilvusPartitionReaderFactory.requestedExtraColumns(optionsMap)

  private def isMetadataExtraField(name: String): Boolean =
    MilvusPartitionReaderFactory.isMetadataExtraField(
      name,
      requestedExtraColumns
    )

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    partition match {
      case p: MilvusStorageV3InputPartition =>
        logInfo(
          s"Creating V3 reader for partition with segmentID=${p.segmentID}"
        )

        val v2Schema = StructType(schema.fields.filterNot { field =>
          isMetadataExtraField(field.name)
        })

        // Deserialize the protobuf schema
        val milvusSchema = CollectionSchema.parseFrom(p.milvusSchemaBytes)

        // Create MilvusLoonPartitionReader directly
        val underlyingReader = new MilvusLoonPartitionReader(
          v2Schema,
          p.manifestPath,
          milvusSchema,
          p.milvusOption,
          optionsMap,
          p.topK,
          p.queryVector,
          p.metricType,
          p.vectorColumn,
          pushedFilters,
          p.readVersion,
          p.applyDeletes,
          p.deletePlan
        )

        val hasMetadataExtraFields = schema.fieldNames.exists { name =>
          isMetadataExtraField(name)
        }

        if (hasMetadataExtraFields) {
          new PartitionReader[InternalRow] {
            override def next(): Boolean = underlyingReader.next()

            override def get(): InternalRow = {
              val row = underlyingReader.get()
              val resultValues = new Array[Any](schema.fields.length)
              var readIdx = 0

              schema.fields.zipWithIndex.foreach { case (field, writeIdx) =>
                field.name match {
                  case MilvusOption.MilvusExtraColumnPartition =>
                    resultValues(writeIdx) =
                      MilvusPartitionReaderFactory.stringValue(p.partitionName)
                  case MilvusOption.MilvusExtraColumnSegmentID =>
                    resultValues(writeIdx) = p.segmentID
                  case MilvusOption.MilvusExtraColumnRowOffset =>
                    resultValues(writeIdx) =
                      underlyingReader.lastReturnedRowOffset
                  case _ =>
                    resultValues(writeIdx) = row.get(readIdx, field.dataType)
                    readIdx += 1
                }
              }

              InternalRow.fromSeq(resultValues.toSeq)
            }

            override def close(): Unit = underlyingReader.close()
          }
        } else {
          underlyingReader
        }

      case p: MilvusPackedV2InputPartition =>
        logInfo(
          s"Creating packed-V2 reader for segmentID=${p.segmentID} " +
            s"with ${p.columnGroups.size} column group(s)"
        )

        val innerSchema = StructType(schema.fields.filterNot { field =>
          isMetadataExtraField(field.name)
        })

        val milvusSchema = CollectionSchema.parseFrom(p.milvusSchemaBytes)

        val inheritedDeletePlan = p.inheritedDeletePlanPartitionId
          .map(partitionId =>
            MilvusDeltaLogReader.effectiveInheritedDeletePlan(
              partitionId,
              packedV2DeleteContext.inheritedPlansByPartition
            )
          )
          .getOrElse(MilvusDeletePlan.empty)
        val effectiveDeletePlan =
          MilvusDeletePlan.union(inheritedDeletePlan, p.deletePlan)

        val underlying = new MilvusPackedV2PartitionReader(
          innerSchema,
          p.columnGroups,
          milvusSchema,
          p.milvusOption,
          p.neededColumnFieldIds,
          p.applyDeletes,
          effectiveDeletePlan
        )

        val hasMetadataExtraFields = schema.fieldNames.exists { name =>
          isMetadataExtraField(name)
        }

        if (hasMetadataExtraFields) {
          new PartitionReader[InternalRow] {
            override def next(): Boolean = underlying.next()

            override def get(): InternalRow = {
              val row = underlying.get()
              val out = new Array[Any](schema.fields.length)
              var readIdx = 0

              schema.fields.zipWithIndex.foreach { case (field, writeIdx) =>
                field.name match {
                  case MilvusOption.MilvusExtraColumnPartition =>
                    out(writeIdx) = MilvusPartitionReaderFactory.stringValue(
                      p.partitionID.toString
                    )
                  case MilvusOption.MilvusExtraColumnSegmentID =>
                    out(writeIdx) = p.segmentID
                  case MilvusOption.MilvusExtraColumnRowOffset =>
                    out(writeIdx) = underlying.lastReturnedRowOffset
                  case _ =>
                    out(writeIdx) = row.get(readIdx, field.dataType)
                    readIdx += 1
                }
              }

              InternalRow.fromSeq(out.toSeq)
            }

            override def close(): Unit = underlying.close()
          }
        } else {
          underlying
        }

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported partition type: ${partition.getClass.getName}. " +
            "This connector requires Milvus 2.6+ (Storage V2)."
        )
    }
  }
}
