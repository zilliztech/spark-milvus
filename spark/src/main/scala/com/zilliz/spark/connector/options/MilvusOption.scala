package com.zilliz.spark.connector.options

import scala.collection.Map

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.expr.{FilterExpr, PlanParser}
import com.zilliz.milvus.storage.read.plan.ReadLimits

case class MilvusOption(
    fieldIDs: String = "",
    extraColumns: Seq[String] = Seq.empty,
    options: Map[String, String] = Map.empty,
    readLimits: ReadLimits = ReadLimits.Default,
    milvusFilter: Option[FilterExpr] = None
)

object MilvusOption {
  // Constants for map keys
  val MilvusPartitions = "milvus.partitions"
  val MilvusSegments = "milvus.segments"
  val ReadBatchMaxRows = "milvus.read.batch.max.rows"
  val ReadBatchMaxBytes = "milvus.read.batch.max.bytes"
  val ReadArrowMaxBytes = "milvus.read.arrow.max.bytes"
  val MilvusFilter = "milvus.filter"

  val MilvusExtraColumns = "milvus.extra.columns"
  val MilvusExtraColumnSegmentID = "_segment_id"
  val MilvusExtraColumnRowOffset = "_row_offset"
  val MilvusExtraColumnTimestamp = "_timestamp"

  /** The stored position of a row in its snapshot, requested with
    * [[ReadIncludeRowId]] rather than [[MilvusExtraColumns]].
    */
  val MilvusRowIdColumn = "_milvus_row_id"
  private[connector] val MilvusExtraColumnSegmentIDAlias = "$segment_id"
  private[connector] val MilvusExtraColumnRowOffsetAlias = "$row_offset"
  private val MilvusExtraColumnSegmentIDBareAlias = "segment_id"
  private val MilvusExtraColumnRowOffsetBareAlias = "row_offset"

  private val SupportedExtraColumns = Set(
    MilvusExtraColumnSegmentID,
    MilvusExtraColumnRowOffset,
    MilvusExtraColumnTimestamp
  )

  private[connector] def normalizeExtraColumnName(name: String): String =
    name match {
      case MilvusExtraColumnSegmentIDAlias |
          MilvusExtraColumnSegmentIDBareAlias =>
        MilvusExtraColumnSegmentID
      case MilvusExtraColumnRowOffsetAlias |
          MilvusExtraColumnRowOffsetBareAlias =>
        MilvusExtraColumnRowOffset
      case other => other
    }

  // reader config
  val ReaderFieldIDs = "fieldIDs"

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
  val ReadApplyDeletes = "milvus.read.apply.deletes"
  val ReadVectorRaw = "milvus.read.vector.raw"
  val ReadColumnar = "milvus.read.columnar"

  /** What a load reads. Absent: the collection's live rows. `physical`: every
    * stored row, including rows marked deleted, which a column replacement
    * write has to cover. `write_result`: the receipt a column replacement write
    * published at the load path, not collection data.
    */
  val ReadMode = "milvus.read.mode"
  val ReadModePhysical = "physical"
  val ReadModeWriteResult = "write_result"

  /** `true` adds [[MilvusRowIdColumn]] to a physical read. */
  val ReadIncludeRowId = "milvus.read.include_row_id"

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

  /** A physical read keeps rows marked deleted, so it cannot also be asked to
    * apply deletes.
    */
  private def readApplyDeletesFrom(
      getOption: String => Option[String]
  ): Boolean =
    if (readModeFrom(getOption).contains(ReadModePhysical)) {
      if (booleanOption(getOption, ReadApplyDeletes, defaultValue = false)) {
        throw new IllegalArgumentException(
          s"'$ReadMode=$ReadModePhysical' reads every stored row including rows marked deleted; " +
            s"remove '$ReadApplyDeletes=true'"
        )
      }
      false
    } else booleanOption(getOption, ReadApplyDeletes, defaultValue = true)

  /** The row identity names a stored position, so only a physical read, whose
    * rows are exactly the stored ones, can carry it.
    */
  private def readIncludeRowIdFrom(
      getOption: String => Option[String]
  ): Boolean = {
    val requested =
      booleanOption(getOption, ReadIncludeRowId, defaultValue = false)
    if (requested && !readModeFrom(getOption).contains(ReadModePhysical)) {
      throw new IllegalArgumentException(
        s"'$ReadIncludeRowId=true' identifies stored rows and requires '$ReadMode=$ReadModePhysical'"
      )
    }
    requested
  }

  def readIncludeRowId(options: Map[String, String]): Boolean =
    readIncludeRowIdFrom(key => OptionParsing.value(options, key))

  def readIncludeRowId(options: CaseInsensitiveStringMap): Boolean =
    readIncludeRowIdFrom(key => Option(options.get(key)))

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

  def readApplyDeletes(options: Map[String, String]): Boolean = {
    readApplyDeletesFrom(key => OptionParsing.value(options, key))
  }

  def readApplyDeletes(options: CaseInsensitiveStringMap): Boolean = {
    readApplyDeletesFrom(key => Option(options.get(key)))
  }

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

  /** Whether the scan hands Spark whole batches instead of rows. Columnar
    * output is the default. Row and columnar readers share the V3 format and
    * delete contracts, covered by fixed read fixtures. `false` selects the row
    * path.
    */
  private def readColumnarFrom(
      getOption: String => Option[String]
  ): Boolean =
    booleanOption(getOption, ReadColumnar, defaultValue = true)

  private def selectedIdsFrom(
      getOption: String => Option[String],
      key: String
  ): Seq[Long] = OptionParsing.nonNegativeLongList(getOption, key)

  /** The synthesized and system columns a read adds to the collection fields:
    * those [[MilvusExtraColumns]] lists, then the row identity when
    * [[ReadIncludeRowId]] asks for it.
    */
  private def extraColumnsFrom(
      getOption: String => Option[String]
  ): Seq[String] = {
    val listed = listedExtraColumnsFrom(getOption)
    if (readIncludeRowIdFrom(getOption)) listed :+ MilvusRowIdColumn
    else listed
  }

  private def listedExtraColumnsFrom(
      getOption: String => Option[String]
  ): Seq[String] = {
    val raw = getOption(MilvusExtraColumns).map(_.trim).getOrElse("")
    if (raw.isEmpty) return Seq.empty
    val values = raw.split(",", -1).toSeq.map(_.trim)
    if (values.exists(_.isEmpty)) {
      throw new IllegalArgumentException(
        s"Option '$MilvusExtraColumns' must be a comma-separated list without empty entries, got '$raw'"
      )
    }
    val normalized = values.map(normalizeExtraColumnName)
    if (normalized.contains(MilvusRowIdColumn)) {
      throw new IllegalArgumentException(
        s"Option '$MilvusExtraColumns' cannot request '$MilvusRowIdColumn'; " +
          s"set '$ReadMode=$ReadModePhysical' and '$ReadIncludeRowId=true'"
      )
    }
    val unsupported = normalized.filterNot(SupportedExtraColumns).distinct
    if (unsupported.nonEmpty) {
      throw new IllegalArgumentException(
        s"Option '$MilvusExtraColumns' contains unsupported column(s) ${unsupported
            .mkString(", ")}; " +
          s"supported columns are ${SupportedExtraColumns.toSeq.sorted.mkString(", ")}"
      )
    }
    normalized.distinct
  }

  def extraColumns(options: Map[String, String]): Seq[String] =
    extraColumnsFrom(key => OptionParsing.value(options, key))

  def extraColumns(options: CaseInsensitiveStringMap): Seq[String] =
    extraColumnsFrom(key => Option(options.get(key)))

  def selectedPartitionIds(options: Map[String, String]): Seq[Long] =
    selectedIdsFrom(
      key => OptionParsing.value(options, key),
      MilvusPartitions
    )

  def selectedPartitionIds(
      options: CaseInsensitiveStringMap
  ): Seq[Long] =
    selectedIdsFrom(key => Option(options.get(key)), MilvusPartitions)

  def selectedSegmentIds(options: Map[String, String]): Seq[Long] =
    selectedIdsFrom(
      key => OptionParsing.value(options, key),
      MilvusSegments
    )

  def selectedSegmentIds(options: CaseInsensitiveStringMap): Seq[Long] =
    selectedIdsFrom(key => Option(options.get(key)), MilvusSegments)

  def readerFieldIds(options: Map[String, String]): Seq[Long] =
    selectedIdsFrom(
      key => OptionParsing.value(options, key),
      ReaderFieldIDs
    )

  def readerFieldIds(options: CaseInsensitiveStringMap): Seq[Long] =
    selectedIdsFrom(key => Option(options.get(key)), ReaderFieldIDs)

  def readColumnar(options: Map[String, String]): Boolean = {
    readColumnarFrom(key => OptionParsing.value(options, key))
  }

  def readColumnar(options: CaseInsensitiveStringMap): Boolean = {
    readColumnarFrom(key => Option(options.get(key)))
  }

  /** Parse public read options. */
  def apply(options: CaseInsensitiveStringMap): MilvusOption = {
    import scala.jdk.CollectionConverters._
    MilvusOption(
      fieldIDs = options.getOrDefault(ReaderFieldIDs, ""),
      extraColumns = extraColumns(options),
      options = options.asScala.toMap,
      readLimits = readLimits(options),
      milvusFilter = parseMilvusFilter(options)
    )
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

  def parseMilvusFilter(
      options: CaseInsensitiveStringMap
  ): Option[FilterExpr] = {
    if (!options.containsKey(MilvusFilter)) return None

    val text = Option(options.get(MilvusFilter)).map(_.trim).getOrElse("")
    if (text.isEmpty) {
      throw new IllegalArgumentException(
        s"Option '$MilvusFilter' must not be empty"
      )
    }

    try Some(PlanParser.parse(text))
    catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Option '$MilvusFilter' is invalid: ${e.getMessage}",
          e
        )
    }
  }

  /** Helper method to convert Map to CaseInsensitiveStringMap and create
    * MilvusOption
    */
  def apply(options: Map[String, String]): MilvusOption = {
    import scala.jdk.CollectionConverters._
    apply(new CaseInsensitiveStringMap(options.asJava))
  }

}
