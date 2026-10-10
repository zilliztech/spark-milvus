package com.zilliz.spark.connector.procedure

import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{
  BooleanType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.Snapshot
import com.zilliz.milvus.storage.write.commit.{
  JobManifest,
  SnapshotTarget,
  SnapshotWriter,
  SourceSnapshot
}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.catalog.MilvusHybridTimestamp
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SnapshotReference,
  StorageOptions
}
import com.zilliz.spark.connector.table.MilvusTables

/** `CALL milvus.system.write_snapshot(...)`: writes the snapshot that describes
  * a build job's output.
  *
  * The segments come from the snapshot the build job recorded in its manifest:
  * the document `build_index` planned against, read again by its key, so the
  * write cannot pick up a snapshot taken between the two calls. The index
  * records come from the same manifest. The table the call names, as
  * `collection` with the call's options or as `table`, a name Spark resolves to
  * a whole Milvus table read with its own options, only reaches the bucket and
  * checks that the collection is the one the job was built over: through
  * `milvus.snapshot.path` when the options give one, which then has to name the
  * recorded document, else through the collection id, the table's or the one
  * Milvus reports for the name. What lands on storage is one Avro manifest per
  * segment and the snapshot JSON that names them, in the layout a snapshot read
  * expects (docs/design/architecture/vector-search.html section 2.7,
  * docs/design/architecture/dataframe-api.html section 9).
  *
  * Restoring the result into a Milvus collection is the separate
  * `restore_snapshot` call; this procedure writes the files and returns where
  * they are. A restore refuses a snapshot whose files are not under the root
  * its document's key derives, so by default (`restorable => true`) a write
  * that would name a file outside `output` is refused before anything is
  * written, with the paths named. `restorable => false` declares a snapshot
  * only this connector reads, which may sit anywhere.
  */
object WriteSnapshotProcedure extends Procedure {

  override val name: String = "write_snapshot"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType, required = false),
    Parameter("job", StringType),
    Parameter("input", StringType),
    Parameter("output", StringType, required = false),
    Parameter("snapshot_id", LongType, required = false),
    Parameter("snapshot_name", StringType, required = false),
    Parameter("restorable", BooleanType, required = false),
    Parameter("table", StringType, required = false)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("snapshot", StringType, nullable = false),
      StructField("snapshot_id", LongType, nullable = false),
      StructField("snapshot_name", StringType, nullable = false),
      StructField("segments", IntegerType, nullable = false),
      StructField("indexes", IntegerType, nullable = false),
      StructField("bytes", LongType, nullable = false)
    )
  )

  override def run(args: ProcedureArgs): Seq[Row] = {
    val tableName = ProcedureSupport.tableName(args, name)
    val input = prefix(args.string("input"), "input")
    val output =
      args.stringOpt("output").map(prefix(_, "output")).getOrElse(input)
    val jobId = args.string("job").trim
    require(
      StagingLayout.isSafeJobId(jobId),
      s"'job' is one storage key component, not '$jobId'"
    )
    val read = tableName.map(MilvusTables.named(SparkSession.active, _))
    val (collection, options) = read match {
      case Some(table) =>
        (
          table.table.snapshot.schema.name,
          table.options.asCaseSensitiveMap().asScala.toMap
        )
      case None =>
        val (database, collection) =
          ProcedureSupport.parseCollection(args.string("collection"), name)
        (
          collection,
          ProcedureSupport.collectionOptions(
            args.options,
            database,
            collection,
            name
          )
        )
    }
    ProcedureSupport.rejectFilter(
      new CaseInsensitiveStringMap(options.asJava),
      name
    )
    require(
      !MilvusOption.isBackupMode(options),
      "write_snapshot copies the snapshot document a build job recorded; a backup export is not one"
    )
    val snapshotPath = StorageOptions
      .optionValue(options, MilvusOption.SnapshotPath)
      .map(_.trim)
      .filter(_.nonEmpty)
    require(
      read.nonEmpty || snapshotPath.nonEmpty || MilvusOption(
        options
      ).uri.trim.nonEmpty,
      s"write_snapshot checks the collection through '${MilvusOption.MilvusUri}', or through " +
        s"'${MilvusOption.SnapshotPath}' naming the snapshot the job was built from; give one"
    )

    // The same resolved `fs.*` bag a write uses, bound to the bucket the table's
    // snapshot is in, or a snapshot path names, else the options' bucket: where
    // build_index committed.
    val properties = StorageOptions.writeStorageProperties(
      options,
      read
        .map(_.table.snapshot.bucket)
        .orElse(
          snapshotPath
            .flatMap(
              StorageOptions.snapshotS3BucketForRelativePaths(_, options)
            )
        )
        .getOrElse("")
    )
    val store = NativeObjectStore.Factory(properties).open()
    try {
      val manifestKey = StagingLayout(input, jobId).manifest
      val manifest = jobManifest(store, manifestKey)
      val source = SnapshotWriter.recordedSource(manifest, manifestKey)
      val local = isLocal(options)
      val bucket = properties.getOrElse(StorageProperties.BucketName, "")
      require(
        local || source.bucket == bucket,
        s"Job $jobId was built over a snapshot in bucket '${source.bucket}'; the options reach bucket '$bucket'"
      )
      snapshotPath match {
        case Some(path) =>
          checkPath(
            path,
            bucket,
            properties.getOrElse(StorageProperties.Address, ""),
            local,
            source,
            jobId
          )
        case None =>
          checkCollectionId(
            read.fold(collectionIdOf(args))(_.table.snapshot.collectionId),
            source,
            jobId
          )
      }
      val snapshot = SnapshotWriter.recordedSnapshot(
        source,
        store,
        _ => recorded(options, source)
      )
      val nowMillis = System.currentTimeMillis()
      val snapshotId = args.longOpt("snapshot_id").getOrElse(nowMillis)
      val target = SnapshotTarget(
        rootPath = output,
        collectionId = source.collectionId,
        snapshotId = snapshotId,
        name = args
          .stringOpt("snapshot_name")
          .map(_.trim)
          .filter(_.nonEmpty)
          .getOrElse(s"$collection-$snapshotId"),
        createTs = MilvusHybridTimestamp.ofMillis(nowMillis)
      )
      val written = SnapshotWriter.write(
        snapshot,
        manifest.indexes,
        target,
        store,
        source.key,
        restorable = args.booleanOpt("restorable").getOrElse(true)
      )
      Seq(
        Row(
          written.metadataKey,
          target.snapshotId,
          target.name,
          written.manifestKeys.size,
          manifest.indexes.size,
          written.bytes
        )
      )
    } finally store.close()
  }

  /** A snapshot path the call still gives has to name the recorded document:
    * the same key, and on object storage the same bucket.
    */
  private[procedure] def checkPath(
      path: String,
      bucket: String,
      endpoint: String,
      local: Boolean,
      source: SourceSnapshot,
      jobId: String
  ): Unit = {
    val located = StoragePath.parseMilvus(path, bucket, endpoint)
    require(
      located.key == source.key && (local || located.bucket == source.bucket),
      s"'${MilvusOption.SnapshotPath}' names '$path'; job $jobId was built from " +
        s"'${source.key}' in bucket '${source.bucket}'"
    )
  }

  private[procedure] def checkCollectionId(
      collectionId: Long,
      source: SourceSnapshot,
      jobId: String
  ): Unit =
    require(
      collectionId == source.collectionId,
      s"The call names collection $collectionId; job $jobId was built over collection " +
        s"${source.collectionId}"
    )

  /** The collection id Milvus reports for the name the call gives. */
  private def collectionIdOf(args: ProcedureArgs): Long =
    ProcedureSupport.withClient(args, name) { (target, client) =>
      client.getCollectionInfo(target.database, target.collection) match {
        case Success(info) => info.collectionID
        case Failure(failure) =>
          throw new IllegalArgumentException(
            s"Cannot resolve Milvus collection '${target.database}.${target.collection}': " +
              failure.getMessage,
            failure
          )
      }
    }

  /** The recorded document, read the way a `milvus.snapshot.path` read reads
    * one, from the bucket the job recorded.
    */
  private def recorded(
      options: Map[String, String],
      source: SourceSnapshot
  ): Snapshot = {
    val withPath = options.filterNot { case (key, _) =>
      key.equalsIgnoreCase(MilvusOption.SnapshotPath)
    } + (MilvusOption.SnapshotPath -> source.key)
    val readOptions =
      if (source.bucket.isEmpty) withPath
      else
        withPath.filterNot { case (key, _) =>
          key.equalsIgnoreCase(StorageProperties.BucketName)
        } + (StorageProperties.BucketName -> source.bucket)
    MilvusTables
      .load(
        new CaseInsensitiveStringMap(readOptions.asJava),
        None,
        SnapshotReference.Configured
      )
      .snapshot
  }

  private def isLocal(options: Map[String, String]): Boolean =
    StorageOptions
      .optionValue(options, StorageProperties.StorageType)
      .exists(_.trim.equalsIgnoreCase(StorageProperties.StorageTypeLocal))

  private def jobManifest(
      store: ObjectStore,
      key: String
  ): JobManifest = {
    require(
      store.exists(key),
      s"No job manifest at '$key'; 'input' and 'job' name where a build job committed"
    )
    JobManifest.fromJson(new String(store.readAll(key), UTF_8)) match {
      case Right(manifest) => manifest
      case Left(failure) =>
        throw new IllegalArgumentException(
          s"The job manifest at '$key' cannot be read: ${failure.getMessage}",
          failure
        )
    }
  }

  private def prefix(value: String, parameter: String): String = {
    val trimmed = value.trim.stripSuffix("/")
    require(
      trimmed.nonEmpty && !trimmed.contains("://"),
      s"'$parameter' is a prefix inside the bucket, not a URI: '$value'"
    )
    trimmed
  }
}
