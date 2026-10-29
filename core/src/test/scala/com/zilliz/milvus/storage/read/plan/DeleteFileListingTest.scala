package com.zilliz.milvus.storage.read.plan

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Path}
import java.util.Comparator

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.{FailingObjectStore, LocalObjectStore}
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  DeltaLogFile,
  Segment,
  SegmentLayout,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin
}
import com.zilliz.milvus.storage.snapshot.json.ManifestItemJson
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The driver lists delete files and reads none of them. */
class DeleteFileListingTest extends AnyFunSuite with Matchers {

  private val schema = CollectionSchema(
    name = "c",
    fields = Seq(
      FieldSchema(
        fieldID = 100,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      )
    )
  )

  private val schemaWithoutPrimaryKey = CollectionSchema(
    name = "c",
    fields = Seq(
      FieldSchema(fieldID = 101, name = "value", dataType = DataType.Int64)
    )
  )

  private def snapshotOf(
      v3: Seq[ManifestItemJson] = Seq.empty,
      collectionSchema: CollectionSchema = schema,
      bucket: String = ""
  ): Snapshot =
    SnapshotCatalog
      .fromLists(
        name = "s",
        collectionId = 10L,
        createdAt = None,
        partitionIds = Seq(20L),
        schemaBytes = collectionSchema.toByteArray,
        v3Items = v3,
        bucket = bucket,
        origin = SnapshotOrigin.Options
      )
      .fold(e => throw e, identity)

  private def unpinnedSnapshot(
      basePath: String,
      collectionSchema: CollectionSchema = schema
  ): Snapshot =
    snapshotOf(collectionSchema = collectionSchema).copy(segments =
      Seq(
        Segment(
          id = 30L,
          partitionId = 20L,
          storageVersion = 3,
          rows = None,
          layout = SegmentLayout.Manifest(basePath, -1L),
          deletes = DeleteFiles.InManifest
        )
      )
    )

  private def withDir(f: Path => Unit): Unit = {
    val dir = Files.createTempDirectory("delete-listing")
    try f(dir)
    finally
      Files
        .walk(dir)
        .sorted(Comparator.reverseOrder[Path]())
        .forEach(p => Files.delete(p))
  }

  private val manifestSchema = new Schema.Parser().parse("""
      {"type":"record","name":"Manifest","namespace":"milvus_storage","fields":[
        {"name":"delta_logs","type":{"type":"array","items":{"type":"record","name":"DeltaLog","fields":[
          {"name":"path","type":"string"},{"name":"type","type":"int"},{"name":"num_entries","type":"long"}]}}}]}
    """)

  private def writeManifest(
      dir: Path,
      basePath: String,
      version: Long,
      logs: Seq[(String, Int, Long)]
  ): Unit = {
    val deltaSchema =
      manifestSchema.getField("delta_logs").schema().getElementType
    val rec = new GenericData.Record(manifestSchema)
    val arr = new GenericData.Array[GenericRecord](
      logs.size,
      manifestSchema.getField("delta_logs").schema()
    )
    logs.foreach { case (path, logType, entries) =>
      val log = new GenericData.Record(deltaSchema)
      log.put("path", path)
      log.put("type", logType)
      log.put("num_entries", entries)
      arr.add(log)
    }
    rec.put("delta_logs", arr)
    val out = new ByteArrayOutputStream()
    val writer =
      new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
    writer.create(manifestSchema, out)
    writer.append(rec)
    writer.close()
    val target = dir.resolve(s"$basePath/_metadata/manifest-$version.avro")
    Files.createDirectories(target.getParent)
    Files.write(target, out.toByteArray)
  }

  private def writeUnparseableManifest(
      dir: Path,
      basePath: String,
      version: Long
  ): Unit = {
    val target = dir.resolve(s"$basePath/_metadata/manifest-$version.avro")
    Files.createDirectories(target.getParent)
    Files.write(target, Array[Byte](1, 2, 3))
  }

  test(
    "a V3 segment's files come from its manifest, and the version resolved is returned"
  ) {
    withDir { dir =>
      val base = "files/insert_log/10/20/30"
      writeManifest(dir, base, 3L, Seq(("9001", 0, 4L), ("ignored", 1, 1L)))
      // a listed version wins over the latest on disk
      writeManifest(dir, base, 4L, Seq(("9002", 0, 1L)))
      val store = new LocalObjectStore(dir.toString)
      val pinned = DeleteFileListing
        .of(
          snapshotOf(v3 =
            Seq(ManifestItemJson(30L, s"""{"ver":3,"base_path":"$base"}"""))
          ),
          applyDeletes = true,
          "",
          store
        )
        .fold(e => throw e, identity)
      pinned.v3BySegment shouldBe Map(
        30L -> Seq(DeltaLogFile(0L, s"$base/_delta/9001", 4L))
      )
      pinned.v3ReadVersions shouldBe Map(30L -> 3L)

      val latest = DeleteFileListing
        .of(
          unpinnedSnapshot(base),
          applyDeletes = true,
          "",
          store
        )
        .fold(e => throw e, identity)
      latest.v3ReadVersions shouldBe Map(30L -> 4L)
      latest.v3BySegment shouldBe Map(
        30L -> Seq(DeltaLogFile(0L, s"$base/_delta/9002", 1L))
      )
    }
  }

  test(
    "V3 L0 deletes follow source channel and partition after segment pruning"
  ) {
    withDir { dir =>
      // (id, partition, channel, delete-only)
      val segments = Seq(
        (30L, 20L, "a", false),
        (31L, 21L, "a", false),
        (32L, 20L, "b", false),
        (40L, 20L, "a", true),
        (41L, -1L, "a", true),
        (42L, 20L, "b", true)
      )
      val items = segments.map { case (id, partition, _, l0) =>
        val base = s"files/insert_log/10/$partition/$id"
        writeManifest(
          dir,
          base,
          1L,
          if (l0) Seq((s"delete-$id", 0, 1L)) else Seq.empty
        )
        ManifestItemJson(id, s"""{"ver":1,"base_path":"$base"}""")
      }
      val initial = snapshotOf(v3 = items)
      val snapshot = initial.copy(
        partitionIds = Seq(20L, 21L),
        segments = initial.segments.zip(segments).map {
          case (segment, (_, _, channel, l0)) =>
            segment.copy(channelName = channel, isDeleteOnly = l0)
        }
      )
      val store = new LocalObjectStore(dir.toString)
      def list(value: Snapshot) =
        DeleteFileListing.of(value, true, "", store).fold(throw _, identity)
      def keys(listing: DeleteFileListing, id: Long) =
        listing.filesFor(id).map(_.logPath.split('/').last).toSet
      val listed = list(snapshot)
      keys(listed, 30L) shouldBe Set("delete-40", "delete-41")
      keys(listed, 31L) shouldBe Set("delete-41")
      keys(listed, 32L) shouldBe Set("delete-42")
      val narrowed = snapshot.narrow(Seq.empty, Seq(30L))
      keys(list(narrowed), 30L) shouldBe Set("delete-40", "delete-41")
      val pruned = snapshot.retainDataSegments(Set(30L))
      keys(list(pruned), 30L) shouldBe Set("delete-40", "delete-41")
      ReadPlan
        .of(snapshot, _ => Map.empty, applyDeletes = true, deletes = listed)
        .specs
        .map(_.segmentId) shouldBe Seq(30L, 31L, 32L)
      val disabled =
        DeleteFileListing.of(snapshot, false, "", store).fold(throw _, identity)
      disabled.inheritedBySegment shouldBe empty
      val missingChannel = snapshot.copy(segments =
        snapshot.segments.map(_.copy(channelName = ""))
      )
      DeleteFileListing.of(missingChannel, true, "", store).isLeft shouldBe true
    }
  }

  test("disabled deletes do not resolve an unpinned L0 manifest") {
    val items = Seq(
      ManifestItemJson(
        30L,
        """{"ver":3,"base_path":"files/insert_log/10/20/30"}"""
      )
    )
    Seq((schema, false), (schemaWithoutPrimaryKey, true)).foreach {
      case (collection, applyDeletes) =>
        val initial = snapshotOf(v3 = items, collectionSchema = collection)
        val snapshot = initial.copy(segments =
          initial.segments.map(_.copy(channelName = "source")) :+
            Segment(
              id = 41L,
              partitionId = -1L,
              storageVersion = 3,
              rows = None,
              layout = SegmentLayout.Manifest("files/insert_log/10/-1/41", -1L),
              deletes = DeleteFiles.InManifest,
              isDeleteOnly = true,
              channelName = "source"
            )
        )
        DeleteFileListing.requiresStore(snapshot, applyDeletes) shouldBe false
        val listing = DeleteFileListing
          .of(
            snapshot,
            applyDeletes,
            "",
            new FailingObjectStore(
              new IllegalStateException("must not be opened")
            )
          )
          .fold(throw _, identity)
        listing.v3ReadVersions shouldBe Map(30L -> 3L)
        listing.inheritedBySegment shouldBe empty
        val plan =
          ReadPlan.of(snapshot, _ => Map.empty, applyDeletes, deletes = listing)
        plan.specs.map(_.segmentId) shouldBe Seq(30L)
        plan.specs.head.readVersionOrLatest shouldBe 3L
    }
  }

  test("version zero data and L0 segments require no storage access") {
    val items = Seq(30L, 41L).map { id =>
      ManifestItemJson(
        id,
        s"""{"ver":0,"base_path":"files/insert_log/10/20/$id"}"""
      )
    }
    Seq((schema, true), (schema, false), (schemaWithoutPrimaryKey, true))
      .foreach { case (collection, applyDeletes) =>
        val initial = snapshotOf(v3 = items, collectionSchema = collection)
        val snapshot = initial.copy(segments =
          initial.segments.map(segment =>
            segment.copy(
              isDeleteOnly = segment.id == 41L,
              channelName = "source"
            )
          )
        )
        DeleteFileListing.requiresStore(snapshot, applyDeletes) shouldBe false
        val listing = DeleteFileListing
          .of(
            snapshot,
            applyDeletes,
            "",
            new FailingObjectStore(
              new IllegalStateException("must not be opened")
            )
          )
          .fold(throw _, identity)
        val listedIds =
          if (applyDeletes && collection == schema) Set(30L, 41L) else Set(30L)
        listing.v3ReadVersions shouldBe listedIds.map(_ -> 0L).toMap
        listing.v3Rows shouldBe listedIds.map(_ -> 0L).toMap
        listing.v3BySegment shouldBe empty
        listing.inheritedBySegment shouldBe empty
        ReadPlan
          .of(snapshot, _ => Map.empty, applyDeletes, deletes = listing)
          .isEmpty shouldBe true
      }
  }

  test("version zero ignores later data and L0 manifests in a mixed snapshot") {
    withDir { dir =>
      val items = Seq((30L, 0L), (31L, 1L), (41L, 0L)).map {
        case (id, version) =>
          val base = s"files/insert_log/10/20/$id"
          writeManifest(dir, base, 1L, Seq((s"delete-$id", 0, 1L)))
          ManifestItemJson(id, s"""{"ver":$version,"base_path":"$base"}""")
      }
      val initial = snapshotOf(v3 = items)
      val snapshot = initial.copy(segments =
        initial.segments.map(segment =>
          segment.copy(isDeleteOnly = segment.id == 41L, channelName = "source")
        )
      )
      val listing = DeleteFileListing
        .of(
          snapshot,
          true,
          "",
          new LocalObjectStore(dir.toString)
        )
        .fold(throw _, identity)
      listing.v3ReadVersions shouldBe Map(30L -> 0L, 31L -> 1L, 41L -> 0L)
      listing.v3Rows shouldBe Map(30L -> 0L, 41L -> 0L)
      listing.inheritedBySegment shouldBe empty
      listing.filesFor(30L) shouldBe empty
      listing.filesFor(31L).map(_.logPath) shouldBe Seq(
        "files/insert_log/10/20/31/_delta/delete-31"
      )
      val plan = ReadPlan.of(
        snapshot,
        _ => Map.empty,
        applyDeletes = true,
        deletes = listing
      )
      plan.specs.map(_.segmentId) shouldBe Seq(31L)
      plan.specs.head.deletes shouldBe DeleteSource.Files(listing.filesFor(31L))
    }
  }

  test("a V3 delete-log endpoint URI becomes a bucket-relative key") {
    withDir { dir =>
      val base = "files/insert_log/10/20/30"
      val key = s"$base/_delta/9001"
      writeManifest(
        dir,
        base,
        3L,
        Seq((s"s3://minio:9000/test-bucket/$key", 0, 4L))
      )

      val listing = DeleteFileListing
        .of(
          snapshotOf(
            v3 = Seq(
              ManifestItemJson(
                30L,
                s"""{"ver":3,"base_path":"$base"}"""
              )
            ),
            bucket = "test-bucket"
          ),
          applyDeletes = true,
          "test-bucket",
          new LocalObjectStore(dir.toString),
          endpoint = "minio:9000"
        )
        .fold(e => throw e, identity)

      listing.v3BySegment shouldBe Map(
        30L -> Seq(DeltaLogFile(0L, key, 4L))
      )
    }
  }

  test("an unpinned V3 segment with no manifest fails closed") {
    withDir { dir =>
      val base = "files/insert_log/10/20/30"
      Seq(true, false).foreach { applyDeletes =>
        val result = DeleteFileListing.of(
          unpinnedSnapshot(base),
          applyDeletes,
          "",
          new LocalObjectStore(dir.toString)
        )
        withClue(s"applyDeletes=$applyDeletes") {
          result.isLeft shouldBe true
          val message = result.left.toOption.get.getMessage
          message should include("segment 30")
          message should include(base)
          message should include("latest manifest version must be positive")
        }
      }
    }
  }

  test(
    "a listed V3 version needs no storage access when deletes are disabled"
  ) {
    val listing = DeleteFileListing
      .of(
        snapshotOf(v3 =
          Seq(ManifestItemJson(30L, """{"ver":3,"base_path":"files/x"}"""))
        ),
        applyDeletes = false,
        "",
        new FailingObjectStore(new IllegalStateException("must not be opened"))
      )
      .fold(e => throw e, identity)
    listing shouldBe DeleteFileListing.empty.copy(
      v3ReadVersions = Map(30L -> 3L)
    )
  }

  test(
    "an internal unpinned V3 version is pinned without parsing its manifest when deletes are disabled"
  ) {
    withDir { dir =>
      val base = "files/insert_log/10/20/30"
      writeUnparseableManifest(dir, base, 4L)
      val listing = DeleteFileListing
        .of(
          unpinnedSnapshot(base),
          applyDeletes = false,
          "",
          new LocalObjectStore(dir.toString)
        )
        .fold(e => throw e, identity)

      listing shouldBe DeleteFileListing.empty.copy(
        v3ReadVersions = Map(30L -> 4L)
      )
    }
  }

  test(
    "a schema without a primary key still pins V3 without listing any delete file"
  ) {
    withDir { dir =>
      val base = "files/insert_log/10/20/30"
      writeUnparseableManifest(dir, base, 5L)
      val listing = DeleteFileListing
        .of(
          unpinnedSnapshot(base, collectionSchema = schemaWithoutPrimaryKey),
          applyDeletes = true,
          "",
          new LocalObjectStore(dir.toString)
        )
        .fold(e => throw e, identity)

      listing shouldBe DeleteFileListing.empty.copy(
        v3ReadVersions = Map(30L -> 5L)
      )
    }
  }

  test("a manifest that cannot be read is an error, not an empty list") {
    val result = DeleteFileListing.of(
      snapshotOf(v3 =
        Seq(ManifestItemJson(30L, """{"ver":3,"base_path":"files/x"}"""))
      ),
      applyDeletes = true,
      "",
      new FailingObjectStore(new java.io.IOException("storage down"))
    )
    result.isLeft shouldBe true
    result.left.toOption.get.getMessage should include(
      "cannot list the delete files of segment 30"
    )
  }
}
