package com.zilliz.milvus.storage.schema

/** The field-metadata key names.
  *
  * Neither Arrow nor Spark has a way to express a Milvus type: Arrow only knows
  * a column is FixedSizeBinary, Spark only knows it is BinaryType, and neither
  * knows it is a 128-dimension FloatVector. These two keys are where the Milvus
  * type and dimension ride along on the field. The read and write paths, the
  * Arrow side and the Spark side all share one set of key names, so they belong
  * to core.
  */
object FieldMetadata {

  /** The Milvus DataType enum value, stored as a Long. */
  val MilvusDataTypeMetadataKey = "milvus.data_type"

  /** The dimension of a dense vector, stored as a Long. A nullable dense vector
    * lands as variable-width binary, and this is what recovers its width.
    */
  val MilvusVectorDimensionMetadataKey = "milvus.vector_dim"

  /** The Milvus field id, which names the column in a written segment. */
  val MilvusFieldIdMetadataKey = "milvus.field_id"

  /** Present and true on the primary key, the partition key and the clustering
    * key; the column-group split puts those with the system fields.
    */
  val MilvusPrimaryKeyMetadataKey = "milvus.primary_key"
  val MilvusPartitionKeyMetadataKey = "milvus.partition_key"
  val MilvusClusteringKeyMetadataKey = "milvus.clustering_key"
}
