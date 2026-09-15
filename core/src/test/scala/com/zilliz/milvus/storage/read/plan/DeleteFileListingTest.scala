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
  DeltaLogFile,
  Segment,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin,
  V2ColumnGroup
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

  private def snapshotOf(
      v3: Seq[ManifestItemJson] = Seq.empty,
      v2: Seq[Segment] = Seq.empty
  ): Snapshot =
    SnapshotCatalog
      .fromLists(
        name = "s",
        collectionId = 10L,
        createdAt = None,
        partitionIds = Seq(20L),
        schemaBytes = schema.toByteArray,
        v3Items = v3,
        v2Segments = v2,
        bucket = "",
        origin = SnapshotOrigin.Options
      )
      .fold(e => throw e, identity)

  private val group = V2ColumnGroup(
    fieldIds = Seq(100L),
    filePaths = Seq("files/insert_log/10/20/31/100/1.parquet"),
    fileRowCounts = Seq(1L)
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

  test("V2 segments list their own files, L0 segments list their partition's") {
    val own = DeltaLogFile(5L, "files/delta_log/10/20/31/5", 2L)
    val l0 = DeltaLogFile(6L, "files/delta_log/10/20/32/6", 3L)
    val snapshot = snapshotOf(v2 =
      Seq(
        Segment.v2(31L, 20L, 1L, Seq(group), Seq(own)),
        Segment.v2(32L, 20L, 0L, Seq.empty, Seq(l0))
      )
    )
    val listing = DeleteFileListing
      .of(snapshot, applyDeletes = true, "", new LocalObjectStore())
      .fold(e => throw e, identity)
    listing.v2BySegment shouldBe Map(31L -> Seq(own))
    listing.inheritedByPartition shouldBe Map(20L -> Seq(l0))
    listing.v3BySegment shouldBe empty
    listing.filesFor(31L, 20L) shouldBe Seq(l0, own)
    listing.filesFor(31L, 21L) shouldBe Seq(own)
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

  test("a V3 segment with no manifest yet lists nothing and pins nothing") {
    withDir { dir =>
      val listing = DeleteFileListing
        .of(
          snapshotOf(v3 =
            Seq(ManifestItemJson(30L, "files/insert_log/10/20/30"))
          ),
          applyDeletes = true,
          "",
          new LocalObjectStore(dir.toString)
        )
        .fold(e => throw e, identity)
      listing shouldBe DeleteFileListing.empty
    }
  }

  test("applyDeletes=false opens nothing") {
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
    listing shouldBe DeleteFileListing.empty
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
