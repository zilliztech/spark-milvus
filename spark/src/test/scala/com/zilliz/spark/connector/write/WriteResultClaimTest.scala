package com.zilliz.spark.connector.write

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, NoSuchFileException}
import scala.collection.mutable

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.{FileInfo, WritableObjectStore}
import io.milvus.storage.MilvusStorageException

/** Exercises immutable operation and result records without a backend. */
class WriteResultClaimTest extends AnyFunSuite with Matchers {
  private val location =
    WriteResultLocation("run/result", Map.empty, "run/result")
  private val mapper = new ObjectMapper()

  private def record(kind: String, operationId: String): ObjectNode =
    mapper
      .createObjectNode()
      .put("format", "milvus-column-write")
      .put("format_version", 1)
      .put("record", kind)
      .put("operation_id", operationId)
      .put("payload", "original")

  private def createOperation(store: MemoryStore, operationId: String): Unit =
    WriteResultRecords.createRecord(
      location,
      location.operationKey,
      record("operation", operationId),
      store
    )

  private final class MemoryStore extends WritableObjectStore {
    private val objects = mutable.Map.empty[String, Array[Byte]]
    val creates = mutable.ArrayBuffer.empty[String]
    val lookups = mutable.ArrayBuffer.empty[String]
    val reads = mutable.ArrayBuffer.empty[String]
    var createFailure: Option[Throwable] = None
    var responseFailure: Option[Throwable] = None
    var lookupFailure: Option[Throwable] = None
    var readFailure: Option[Throwable] = None
    var closed = false

    def seed(key: String, json: String): Unit =
      objects(key) = json.getBytes(StandardCharsets.UTF_8)

    def bytes(key: String): Array[Byte] = objects(key).clone()

    override def create(key: String, data: Array[Byte]): Unit = {
      creates += key
      createFailure.foreach(error => throw error)
      if (objects.contains(key)) throw new FileAlreadyExistsException(key)
      objects(key) = data.clone()
      responseFailure.foreach(error => throw error)
    }

    override def exists(key: String): Boolean = {
      lookups += key
      if (key != location.legacyClaimKey)
        lookupFailure.foreach(error => throw error)
      objects.contains(key)
    }

    override def readAll(key: String): Array[Byte] = {
      reads += key
      readFailure.foreach(error => throw error)
      objects.getOrElse(key, throw new NoSuchFileException(key)).clone()
    }

    override def write(key: String, data: Array[Byte]): Unit =
      throw new AssertionError("a claim must use conditional create")

    override def size(key: String): Long =
      throw new AssertionError("claim ownership does not need file size")

    override def list(key: String, recursive: Boolean): Seq[FileInfo] =
      throw new AssertionError("claim ownership must use exact keys")

    override def readAt(
        key: String,
        offset: Long,
        length: Long,
        fileSize: Long
    ): Array[Byte] =
      throw new AssertionError("claim ownership reads the complete record")

    override def createDir(key: String, recursive: Boolean): Unit =
      throw new AssertionError("claim records do not create directories")

    override def delete(key: String): Unit =
      throw new AssertionError("claim records must not be deleted")

    override def close(): Unit = closed = true
  }

  test("the operation record claims its address and borrows the store") {
    val store = new MemoryStore
    createOperation(store, "owner")

    location.operationKey shouldBe "run/result/_operation.json"
    store.creates.toSeq shouldBe Seq(location.operationKey)
    mapper.readTree(store.bytes(location.operationKey)) shouldBe
      record("operation", "owner")
    store.reads shouldBe empty
    store.closed shouldBe false
  }

  test("another operation cannot replace an existing claim") {
    val store = new MemoryStore
    store.seed(location.operationKey, record("operation", "first").toString)
    val original = store.bytes(location.operationKey)

    val failure = intercept[IllegalStateException] {
      createOperation(store, "second")
    }

    failure.getMessage should include("already belongs to another column write")
    failure.getCause shouldBe a[FileAlreadyExistsException]
    store.bytes(location.operationKey).toSeq shouldBe original.toSeq
    store.closed shouldBe false
  }

  test(
    "a lost response is recovered from the complete operation or result record"
  ) {
    for (
      (kind, key) <- Seq(
        "operation" -> location.operationKey,
        "result" -> location.resultKey
      )
    ) {
      withClue(s"record=$kind: ") {
        val store = new MemoryStore
        val expected = record(kind, "owner")
          .put("collection_id", 1L)
        store.responseFailure = Some(new IOException("create response lost"))

        WriteResultRecords.createRecord(location, key, expected, store)

        store.creates.toSeq shouldBe Seq(key)
        store.reads.toSeq shouldBe Seq(key)
        mapper.readTree(store.bytes(key)).toString shouldBe expected.toString
        store.closed shouldBe false
      }
    }
  }

  test("a failed create with no stored claim preserves the original failure") {
    val store = new MemoryStore
    val original = new IOException("upload failed")
    store.createFailure = Some(original)

    val failure = intercept[IllegalStateException] {
      createOperation(store, "owner")
    }

    failure.getCause shouldBe original
    failure.getMessage should not include "already belongs"
    store.reads shouldBe empty
    store.closed shouldBe false
  }

  test(
    "a native conflict without an object is not reported as an existing owner"
  ) {
    val store = new MemoryStore
    val original = new MilvusStorageException(102, "AwsErrorConflict")
    store.createFailure = Some(original)

    val failure = intercept[IllegalStateException] {
      createOperation(store, "owner")
    }

    failure.getCause shouldBe original
    failure.getMessage should not include "already belongs"
    store.lookups should contain(location.operationKey)
    store.reads shouldBe empty
  }

  test(
    "a failed ownership lookup or read remains attached to the create failure"
  ) {
    for (failLookup <- Seq(false, true)) {
      withClue(s"failLookup=$failLookup: ") {
        val store = new MemoryStore
        val original = new IOException("upload outcome unknown")
        val inspection = new IOException("claim cannot be inspected")
        store.seed(location.operationKey, record("operation", "owner").toString)
        store.createFailure = Some(original)
        if (failLookup) store.lookupFailure = Some(inspection)
        else store.readFailure = Some(inspection)

        val failure = intercept[IllegalStateException] {
          createOperation(store, "owner")
        }

        failure.getCause shouldBe original
        original.getSuppressed.toSeq shouldBe Seq(inspection)
        store.closed shouldBe false
      }
    }
  }

  test(
    "a corrupt or invalid claim cannot prove ownership after a failed create"
  ) {
    val valid = record("operation", "owner").toString
    val invalid = Seq(
      "not JSON",
      "null",
      "[]",
      valid + " {}",
      valid + " garbage",
      valid.replace("milvus-column-write", "unrelated-format"),
      valid.replace("\"format_version\":1", "\"format_version\":2"),
      valid.replace("\"record\":\"operation\"", "\"record\":\"result\""),
      valid.replace(",\"operation_id\":\"owner\"", ""),
      valid.replace("\"operation_id\":\"owner\"", "\"operation_id\":42"),
      valid.replace("\"operation_id\":\"owner\"", "\"operation_id\":\"\"")
    )
    invalid.foreach { json =>
      withClue(s"claim=$json: ") {
        val store = new MemoryStore
        val original = new IOException("upload outcome unknown")
        store.seed(location.operationKey, json)
        store.createFailure = Some(original)

        val failure = intercept[IllegalStateException] {
          createOperation(store, "owner")
        }

        failure.getCause shouldBe original
        original.getSuppressed should have length 1
        new String(
          store.bytes(location.operationKey),
          StandardCharsets.UTF_8
        ) shouldBe json
      }
    }
  }

  test(
    "unsupported conditional writes fail even when a matching claim exists"
  ) {
    val store = new MemoryStore
    val unsupported =
      new UnsupportedOperationException("conditional uploads unsupported")
    store.seed(location.operationKey, record("operation", "owner").toString)
    store.createFailure = Some(unsupported)

    val failure = intercept[UnsupportedOperationException] {
      createOperation(store, "owner")
    }

    failure shouldBe unsupported
    store.lookups should not contain location.operationKey
    store.reads shouldBe empty
    store.closed shouldBe false
  }

  test(
    "an existing legacy claim blocks the address before creating a new claim"
  ) {
    val store = new MemoryStore
    location.legacyClaimKey shouldBe "run/result/_claim/_metadata/manifest-1.avro"
    // Any object reserves the old address; inspecting or migrating its Avro is unnecessary.
    store.seed(location.legacyClaimKey, "legacy claim bytes")

    intercept[IllegalStateException] {
      createOperation(store, "owner")
    }

    store.lookups should contain(location.legacyClaimKey)
    store.creates shouldBe empty
    store.reads shouldBe empty
    store.closed shouldBe false
  }

  test(
    "publishing the same complete receipt again preserves its stored bytes"
  ) {
    val store = new MemoryStore
    val expected = record("result", "owner")
    WriteResultRecords.createRecord(
      location,
      location.resultKey,
      expected,
      store
    )
    val original = store.bytes(location.resultKey)

    WriteResultRecords.createRecord(
      location,
      location.resultKey,
      expected,
      store
    )

    store.creates.toSeq shouldBe Seq(location.resultKey, location.resultKey)
    store.bytes(location.resultKey).toSeq shouldBe original.toSeq
    store.closed shouldBe false
  }

  test(
    "the same operation cannot change a previously created operation or receipt"
  ) {
    for (
      (kind, key) <- Seq(
        "operation" -> location.operationKey,
        "result" -> location.resultKey
      )
    ) {
      withClue(s"record=$kind: ") {
        val store = new MemoryStore
        val expected = record(kind, "owner")
        store.seed(key, expected.toString)
        val original = store.bytes(key)
        val changed = record(kind, "owner").put("payload", "changed")

        val failure = intercept[IllegalStateException] {
          WriteResultRecords.createRecord(location, key, changed, store)
        }

        failure.getMessage should include("differs")
        failure.getCause shouldBe a[FileAlreadyExistsException]
        store.bytes(key).toSeq shouldBe original.toSeq
      }
    }
  }

  test("a different operation cannot replace a published receipt") {
    val store = new MemoryStore
    store.seed(location.resultKey, record("result", "first").toString)
    val original = store.bytes(location.resultKey)

    val failure = intercept[IllegalStateException] {
      WriteResultRecords.createRecord(
        location,
        location.resultKey,
        record("result", "second"),
        store
      )
    }

    failure.getMessage should include("already belongs to another column write")
    failure.getCause shouldBe a[FileAlreadyExistsException]
    store.bytes(location.resultKey).toSeq shouldBe original.toSeq
  }
}
