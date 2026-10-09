package com.zilliz.spark.connector.procedure

import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{
  BooleanType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.Row

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.NativeObjectStore
import com.zilliz.milvus.storage.write.commit.{
  JobManifest,
  SnapshotTarget,
  SnapshotWriter
}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.catalog.MilvusHybridTimestamp
import com.zilliz.spark.connector.options.{SnapshotReference, StorageOptions}
import com.zilliz.spark.connector.table.MilvusTables

/** `CALL milvus.system.write_snapshot(...)`: writes the snapshot that describes
  * a build job's output.
  *
  * The segments come from the snapshot the options select — the same one
  * `build_index` planned against — and the index records from that job's
  * manifest. What lands on storage is one Avro manifest per segment and the
  * snapshot JSON that names them, in the layout a snapshot read expects
  * (docs/design/architecture/vector-search.html section 2.7).
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
    Parameter("collection", StringType),
    Parameter("job", StringType),
    Parameter("input", StringType),
    Parameter("output", StringType, required = false),
    Parameter("snapshot_id", LongType, required = false),
    Parameter("snapshot_name", StringType, required = false),
    Parameter("restorable", BooleanType, required = false)
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
    val (database, collection) =
      ProcedureSupport.parseCollection(args.string("collection"), name)
    val options = ProcedureSupport.collectionOptions(
      args.options,
      database,
      collection,
      name
    )
    val input = prefix(args.string("input"), "input")
    val output =
      args.stringOpt("output").map(prefix(_, "output")).getOrElse(input)
    val jobId = args.string("job").trim
    require(
      StagingLayout.isSafeJobId(jobId),
      s"'job' is one storage key component, not '$jobId'"
    )
    val table = MilvusTables.load(
      new CaseInsensitiveStringMap(options.asJava),
      None,
      SnapshotReference.Configured
    )
    val nowMillis = System.currentTimeMillis()
    val snapshotId = args.longOpt("snapshot_id").getOrElse(nowMillis)
    val target = SnapshotTarget(
      rootPath = output,
      collectionId = table.snapshot.collectionId,
      snapshotId = snapshotId,
      name = args
        .stringOpt("snapshot_name")
        .map(_.trim)
        .filter(_.nonEmpty)
        .getOrElse(s"$collection-$snapshotId"),
      createTs = MilvusHybridTimestamp.ofMillis(nowMillis)
    )

    // The same resolved `fs.*` bag a write uses, so a read that works cannot
    // leave a snapshot write that fails on the same options.
    val properties =
      StorageOptions.writeStorageProperties(options, table.snapshot.bucket)
    // What this writes is the source snapshot with the job's indexes in it, so
    // the document it came from has to be readable from the same store.
    val sourceKey = SnapshotWriter
      .sourceKeyOf(
        table.snapshot,
        properties.getOrElse(StorageProperties.Address, "")
      )
      .getOrElse(
        throw new IllegalArgumentException(
          s"'$collection' was not read from a snapshot document, so there is nothing to copy: " +
            s"${table.snapshot.origin}"
        )
      )
    val store = NativeObjectStore.Factory(properties).open()
    try {
      val manifest = jobManifest(store, StagingLayout(input, jobId).manifest)
      val written = SnapshotWriter.write(
        table.snapshot,
        manifest.indexes,
        target,
        store,
        sourceKey,
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

  private def jobManifest(
      store: com.zilliz.milvus.storage.io.ObjectStore,
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
