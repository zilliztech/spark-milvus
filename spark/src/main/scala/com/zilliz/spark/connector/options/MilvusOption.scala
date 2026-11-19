package com.zilliz.spark.connector.options

import scala.collection.Map

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.read.plan.ReadLimits

case class MilvusOption(
    options: Map[String, String],
    readLimits: ReadLimits
)

object MilvusOption {
  // Constants for map keys
  val ReadBatchMaxRows = "milvus.read.batch.max.rows"
  val ReadBatchMaxBytes = "milvus.read.batch.max.bytes"
  val ReadArrowMaxBytes = "milvus.read.arrow.max.bytes"

  private val RemovedMilvusFilter = "milvus.filter"
  private val UnsupportedOptions = Seq(
    "milvus.partitions",
    "fieldIDs",
    "milvus.extra.columns",
    "milvus.read.include_row_id",
    "milvus.read.apply.deletes",
    "milvus.read.columnar"
  )

  // s3 config
  val S3Endpoint = "s3.endpoint"
  val S3BucketName = "s3.bucketName"
  val S3PathStyleAccess = "s3.pathStyleAccess"

  // Effective storage aliases; canonical properties are defined in core.
  val FsAddress = "fs.address"
  val FsBucketName = "fs.bucketName"
  val FsRegion = "fs.region"

  val SnapshotMode = "milvus.snapshot.mode" // "true" to enable snapshot mode
  // Only an empty manifest list is accepted; nonempty snapshots use SnapshotPath.
  val SnapshotManifests = "milvus.snapshot.manifests"

  /** A snapshot JSON in the snapshot directory, as an `s3a://` URI or a key
    * relative to `fs.bucket_name`. Snapshot mode without a Milvus service: the
    * schema, the partitions and the segments all come from that file.
    */
  val SnapshotPath = "milvus.snapshot.path"
  val SnapshotCollectionId = "milvus.snapshot.collection.id"
  val SnapshotPartitionIds = "milvus.snapshot.partition.ids"
  val SnapshotSchemaJson =
    "milvus.snapshot.schema.json" // Raw snapshot JSON carrying the collection schema
  val SnapshotSchemaBytes =
    "milvus.snapshot.schema.bytes" // Base64 encoded protobuf CollectionSchema bytes
  val SnapshotMaxJsonBytes = "milvus.snapshot.max.json.bytes"
  val ReadVectorRaw = "milvus.read.vector.raw"

  /** What a load reads. Absent: the collection's live rows. `physical`: every
    * stored row, including rows marked deleted, which a column replacement
    * write has to cover. `write_result`: the receipt a column replacement write
    * published at the load path, not collection data.
    */
  val ReadMode = "milvus.read.mode"
  val ReadModePhysical = "physical"
  val ReadModeWriteResult = "write_result"

  /** What a write does. The one operation is [[WriteOperationReplaceColumns]]:
    * new files for the given columns of every stored row of a fixed snapshot.
    */
  val WriteOperation = "milvus.write.operation"
  val WriteOperationReplaceColumns = "replace_columns"

  private def nonEmptyOption(
      getOption: String => Option[String],
      key: String
  ): Boolean =
    getOption(key).exists(_.trim.nonEmpty)

  private def booleanOption(
      getOption: String => Option[String],
      key: String,
      defaultValue: => Boolean
  ): Boolean = OptionParsing.boolean(getOption, key, defaultValue)

  private def isSnapshotModeFrom(
      getOption: String => Option[String]
  ): Boolean =
    booleanOption(
      getOption,
      SnapshotMode,
      defaultValue = {
        // Only a non-empty hint enables snapshot mode: config templates that
        // keep optional keys with empty values (e.g. milvus.snapshot.manifests="")
        // must not enable a source by themselves.
        nonEmptyOption(getOption, SnapshotManifests) ||
        nonEmptyOption(getOption, SnapshotPath)
      }
    )

  private def validateSnapshotModeOptionsFrom(
      getOption: String => Option[String]
  ): Unit = {
    val explicitSnapshotMode = booleanOption(
      getOption,
      SnapshotMode,
      defaultValue = false
    )
    val hasSnapshotLists = nonEmptyOption(getOption, SnapshotManifests)
    val hasSnapshotPath = nonEmptyOption(getOption, SnapshotPath)
    // A schema alone describes an empty, readable snapshot.
    val hasSchema = nonEmptyOption(getOption, SnapshotSchemaBytes) ||
      nonEmptyOption(getOption, SnapshotSchemaJson)
    if (
      explicitSnapshotMode && !hasSnapshotLists && !hasSnapshotPath && !hasSchema
    ) {
      throw new IllegalArgumentException(
        s"$SnapshotMode=true requires $SnapshotPath, $SnapshotManifests or $SnapshotSchemaBytes"
      )
    }
    if (hasSnapshotPath && hasSnapshotLists) {
      throw new IllegalArgumentException(
        s"$SnapshotPath and $SnapshotManifests are two sources for the same read; give one"
      )
    }
  }

  def isSnapshotMode(options: Map[String, String]): Boolean = {
    isSnapshotModeFrom(key => OptionParsing.value(options, key))
  }

  def isSnapshotMode(options: CaseInsensitiveStringMap): Boolean = {
    isSnapshotModeFrom(key => Option(options.get(key)))
  }

  def validateSnapshotModeOptions(options: Map[String, String]): Unit = {
    validateSnapshotModeOptionsFrom(key => OptionParsing.value(options, key))
  }

  def validateSnapshotModeOptions(options: CaseInsensitiveStringMap): Unit = {
    validateSnapshotModeOptionsFrom(key => Option(options.get(key)))
  }

  private def readModeFrom(
      getOption: String => Option[String]
  ): Option[String] =
    getOption(ReadMode)
      .map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .filter(_.nonEmpty) match {
      case None                                                  => None
      case Some(mode @ (ReadModePhysical | ReadModeWriteResult)) => Some(mode)
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Option '$ReadMode' must be '$ReadModePhysical' or '$ReadModeWriteResult', got '$other'; " +
            "leave it unset to read the collection's live rows"
        )
    }

  def isPhysicalRead(options: Map[String, String]): Boolean =
    readModeFrom(key => OptionParsing.value(options, key))
      .contains(ReadModePhysical)

  def isPhysicalRead(options: CaseInsensitiveStringMap): Boolean =
    readModeFrom(key => Option(options.get(key))).contains(ReadModePhysical)

  def isWriteResultRead(options: Map[String, String]): Boolean =
    readModeFrom(key => OptionParsing.value(options, key))
      .contains(ReadModeWriteResult)

  def isWriteResultRead(options: CaseInsensitiveStringMap): Boolean =
    readModeFrom(key => Option(options.get(key)))
      .contains(ReadModeWriteResult)

  /** The write operation, if the options ask for one. */
  private def writeOperationFrom(
      getOption: String => Option[String]
  ): Option[String] =
    getOption(WriteOperation)
      .map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .filter(_.nonEmpty) match {
      case None                                    => None
      case Some(op @ WriteOperationReplaceColumns) => Some(op)
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Option '$WriteOperation' must be '$WriteOperationReplaceColumns', got '$other'"
        )
    }

  def writeOperation(options: Map[String, String]): Option[String] =
    writeOperationFrom(key => OptionParsing.value(options, key))

  def writeOperation(options: CaseInsensitiveStringMap): Option[String] =
    writeOperationFrom(key => Option(options.get(key)))

  /** Whether vector columns come out as the bytes Milvus stored. Default false:
    * vectors are decoded into Spark's own types. True hands the stored bytes
    * over as BinaryType, which is what a job that feeds them straight to a
    * native library wants — converting to Array[Float] and back again is pure
    * waste there.
    */
  private def readVectorRawFrom(
      getOption: String => Option[String]
  ): Boolean =
    booleanOption(getOption, ReadVectorRaw, defaultValue = false)

  def readVectorRaw(options: Map[String, String]): Boolean = {
    readVectorRawFrom(key => OptionParsing.value(options, key))
  }

  def readVectorRaw(options: CaseInsensitiveStringMap): Boolean = {
    readVectorRawFrom(key => Option(options.get(key)))
  }

  /** Parse public read options. */
  def apply(options: CaseInsensitiveStringMap): MilvusOption = {
    import scala.jdk.CollectionConverters._
    rejectRemovedOptions(options)
    MilvusOption(
      options = options.asScala.toMap,
      readLimits = readLimits(options)
    )
  }

  /** Reject removed snapshot read options, including blank values. */
  def rejectRemovedOptions(options: CaseInsensitiveStringMap): Unit = {
    if (options.containsKey(RemovedMilvusFilter)) {
      throw new IllegalArgumentException(
        s"Option '$RemovedMilvusFilter' was removed; use DataFrame where/filter or SQL WHERE instead, " +
          "which the connector pushes down to the scan"
      )
    }
    rejectUnsupportedOptions(options)
  }

  private[connector] def rejectUnsupportedOptions(
      options: CaseInsensitiveStringMap
  ): Unit =
    UnsupportedOptions.find(options.containsKey).foreach { key =>
      throw new IllegalArgumentException(s"Unsupported option '$key'")
    }

  /** Resource limits shared by public and private storage readers. */
  def readLimits(options: CaseInsensitiveStringMap): ReadLimits = {
    val getOption = (key: String) => OptionParsing.value(options, key)
    ReadLimits(
      OptionParsing.positiveInt(
        getOption,
        ReadBatchMaxRows,
        ReadLimits.DefaultBatchMaxRows
      ),
      OptionParsing.positiveLong(
        getOption,
        ReadBatchMaxBytes,
        ReadLimits.DefaultBatchMaxBytes,
        ReadLimits.MaxBatchBytes
      ),
      OptionParsing.positiveLong(
        getOption,
        ReadArrowMaxBytes,
        ReadLimits.DefaultArrowMaxBytes
      )
    )
  }

  /** Helper method to convert Map to CaseInsensitiveStringMap and create
    * MilvusOption
    */
  def apply(options: Map[String, String]): MilvusOption = {
    import scala.jdk.CollectionConverters._
    apply(new CaseInsensitiveStringMap(options.asJava))
  }

}
