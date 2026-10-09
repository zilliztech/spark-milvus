package com.zilliz.milvus.storage.path

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The seven spellings listed in docs/design/architecture/storage-access.html
  * 1.1, plus the rules the C layer's StorageUri::Parse follows.
  */
class StoragePathTest extends AnyFunSuite with Matchers {

  private val Bucket = "milvus-bucket"

  test("scheme and authority: the authority is the bucket") {
    StoragePath.parse("s3a://a-bucket/files/x") shouldBe
      Located("a-bucket", "files/x")
    StoragePath.parse("s3://a-bucket/files/x") shouldBe
      Located("a-bucket", "files/x")
    StoragePath.parse("oss://a-bucket/files/x") shouldBe
      Located("a-bucket", "files/x")
  }

  test("a URI's own bucket wins over the default") {
    StoragePath.parse("s3a://other/files/x", Bucket) shouldBe
      Located("other", "files/x")
  }

  test("no scheme: the whole path is the key and the caller's bucket is used") {
    StoragePath.parse("files/insert_log/1/2/3/4/5", Bucket) shouldBe
      Located(Bucket, "files/insert_log/1/2/3/4/5")
    StoragePath.parse("_delta/xxx", Bucket) shouldBe
      Located(Bucket, "_delta/xxx")
  }

  test("a leading slash does not make the path absolute") {
    StoragePath.parse("/files/x", Bucket) shouldBe Located(Bucket, "files/x")
    StoragePath.parse("///files/x", Bucket) shouldBe Located(Bucket, "files/x")
  }

  test("a bucket-prefixed path without a scheme is still just a key") {
    // The two spellings are the same shape as strings, so the parser must not
    // guess that the first segment is a bucket. Doing so is what produces
    // a-bucket/a-bucket/files/... once the C layer prepends its own subtree.
    StoragePath.parse("a-bucket/files/x", Bucket) shouldBe
      Located(Bucket, "a-bucket/files/x")
  }

  test(
    "no bucket means no object-storage location: the string stays verbatim"
  ) {
    // This is what lets a bare relative path and a local URI keep resolving
    // against Hadoop's default filesystem during the migration.
    StoragePath.parse("file:///tmp/seg/x") shouldBe
      Located("", "file:///tmp/seg/x")
    StoragePath.parse("files/seg/x") shouldBe Located("", "files/seg/x")
    StoragePath.parse("/files/seg/x") shouldBe Located("", "/files/seg/x")
  }

  test("a supplied bucket turns those into real locations") {
    StoragePath.parse("file:///tmp/seg/x", Bucket) shouldBe
      Located(Bucket, "tmp/seg/x")
    StoragePath.parse("/files/seg/x", Bucket) shouldBe
      Located(Bucket, "files/seg/x")
  }

  test("blank input and key-less URIs are rejected") {
    an[IllegalArgumentException] should be thrownBy StoragePath.parse("")
    an[IllegalArgumentException] should be thrownBy StoragePath.parse("   ")
    an[IllegalArgumentException] should be thrownBy StoragePath.parse(null)
    an[IllegalArgumentException] should be thrownBy StoragePath.parse(
      "s3a://a-bucket"
    )
    an[IllegalArgumentException] should be thrownBy StoragePath.parse(
      "s3a://a-bucket/"
    )
  }

  test("resolve joins a fragment onto the key") {
    val base = Located("b", "files/seg/1")
    StoragePath.resolve(base, "_delta/d1") shouldBe
      Located("b", "files/seg/1/_delta/d1")
    StoragePath.resolve(base, "/_delta/d1") shouldBe
      Located("b", "files/seg/1/_delta/d1")
    StoragePath.resolve(base.copy(key = "files/seg/1/"), "x") shouldBe
      Located("b", "files/seg/1/x")
  }

  test("resolve keeps the base when the fragment is empty") {
    val base = Located("b", "files/seg/1")
    StoragePath.resolve(base, "") shouldBe base
    StoragePath.resolve(base, null) shouldBe base
  }

  test("an absolute fragment replaces the base") {
    val base = Located("b", "files/seg/1")
    StoragePath.resolve(base, "s3a://other/abs/x") shouldBe
      Located("other", "abs/x")
  }

  test("resolve starting from an empty key does not leave a leading slash") {
    StoragePath.resolve(Located("b", ""), "files/x") shouldBe
      Located("b", "files/x")
  }

  test("uri renders back, and accepts the scheme with or without ://") {
    Located("b", "files/x").uri("s3a") shouldBe "s3a://b/files/x"
    Located("b", "files/x").uri("s3a://") shouldBe "s3a://b/files/x"
    Located("b", "files/x").uri("oss") shouldBe "oss://b/files/x"
  }

  test("uri without a bucket hands the string back unchanged") {
    Located("", "files/x").uri("s3a") shouldBe "files/x"
    Located("", "file:///tmp/x").uri("s3a") shouldBe "file:///tmp/x"
  }

  test("a path with no bucket round-trips through parse and uri") {
    Seq("files/seg/x", "/files/seg/x", "file:///tmp/x").foreach { raw =>
      StoragePath.parse(raw).uri("s3a") shouldBe raw
    }
  }

  test("parse and uri round-trip, normalising s3 to the asked-for scheme") {
    StoragePath.parse("s3://a/files/x").uri("s3a") shouldBe "s3a://a/files/x"
  }

  test("parse never treats a user URI authority as an endpoint") {
    StoragePath.parse(
      "https://storage.internal/milvus-bucket/files/x",
      Bucket
    ) shouldBe Located("storage.internal", "milvus-bucket/files/x")
  }

  test("parseMilvus recognizes an endpoint authority with an explicit port") {
    StoragePath.parseMilvus(
      "s3://minio:9000/milvus-bucket/files/x",
      Bucket
    ) shouldBe Located("milvus-bucket", "files/x")
  }

  test("parseMilvus recognizes an exact configured endpoint host") {
    StoragePath.parseMilvus(
      "https://storage.internal/milvus-bucket/files/x",
      Bucket,
      "https://STORAGE.internal:443"
    ) shouldBe Located("milvus-bucket", "files/x")
  }

  test("parseMilvus leaves standard dotted and single-label buckets intact") {
    StoragePath.parseMilvus(
      "s3://bucket.with.dots/milvus-bucket/files/x",
      Bucket,
      "storage.internal"
    ) shouldBe Located("bucket.with.dots", "milvus-bucket/files/x")
    StoragePath.parseMilvus(
      "s3://archive/milvus-bucket/files/x",
      Bucket,
      "storage.internal"
    ) shouldBe Located("archive", "milvus-bucket/files/x")
  }

  test("parseMilvus does not use the configured bucket as a path heuristic") {
    StoragePath.parseMilvus(
      "s3://other-bucket/milvus-bucket/files/x",
      Bucket
    ) shouldBe Located("other-bucket", "milvus-bucket/files/x")
  }

  test("parseMilvus detects ported endpoint paths that java.net.URI rejects") {
    StoragePath.parseMilvus(
      "s3://minio:9000/milvus-bucket/files/key with space",
      Bucket
    ) shouldBe Located("milvus-bucket", "files/key with space")
  }

  test("parseMilvus requires a key after the endpoint and bucket") {
    val error = intercept[IllegalArgumentException] {
      StoragePath.parseMilvus("s3://minio:9000/milvus-bucket", Bucket)
    }
    error.getMessage should include("no key")
  }
}
