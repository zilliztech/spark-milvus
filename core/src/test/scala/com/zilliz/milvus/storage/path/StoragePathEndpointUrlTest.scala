package com.zilliz.milvus.storage.path

import org.scalatest.funsuite.AnyFunSuite

/** Milvus writes a snapshot's location as a path-style endpoint URL
  * (`https://s3.<region>.amazonaws.com/<bucket>/<key>`, DescribeSnapshot's
  * `s3_location`). The endpoint is not the bucket.
  */
class StoragePathEndpointUrlTest extends AnyFunSuite {

  test("https endpoint URL: first path segment is the bucket") {
    val located = StoragePath.parseMilvus(
      "https://s3.us-west-2.amazonaws.com/zilliz-aws-abc/c48b/snapshots/1/metadata/2.json",
      endpoint = "https://s3.us-west-2.amazonaws.com"
    )
    assert(located.bucket == "zilliz-aws-abc")
    assert(located.key == "c48b/snapshots/1/metadata/2.json")
  }

  test(
    "http endpoint URL works the same, and s3a keeps the authority as bucket"
  ) {
    assert(
      StoragePath.parseMilvus("http://minio:9000/b/k/1.json") == Located(
        "b",
        "k/1.json"
      )
    )
    assert(StoragePath.parse("s3a://b/k/1.json") == Located("b", "k/1.json"))
  }

  test("an endpoint URL with only a bucket and no key is refused") {
    val err = intercept[IllegalArgumentException](
      StoragePath.parseMilvus(
        "https://s3.us-west-2.amazonaws.com/bucket-only",
        endpoint = "s3.us-west-2.amazonaws.com"
      )
    )
    assert(err.getMessage.contains("no key"))
  }
}
