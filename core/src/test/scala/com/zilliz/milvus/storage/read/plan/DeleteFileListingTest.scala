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
          snapshotOf(v3 = Seq(ManifestItemJson(30L, base))),
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
          snapshotOf(v3 = Seq(ManifestItemJson(30L, base))),
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
    "a missing V3 version is pinned without parsing its manifest when deletes are disabled"
  ) {
    withDir { dir =>
      val base = "files/insert_log/10/20/30"
      writeUnparseableManifest(dir, base, 4L)
      val listing = DeleteFileListing
        .of(
          snapshotOf(
            v3 = Seq(ManifestItemJson(30L, base))
          ),
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
          snapshotOf(
            v3 = Seq(ManifestItemJson(30L, base)),
            collectionSchema = schemaWithoutPrimaryKey
          ),
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
