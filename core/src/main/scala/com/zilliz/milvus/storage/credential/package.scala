package com.zilliz.milvus.storage

/** Turns user options into the `fs.*` property map the C layer takes.
  *
  * The output is a plain Map[String, String] whose keys are all `fs.*`, or
  * `extfs.<name>.*` when one job reaches more than one bucket. This package
  * validates what is required, rejects blank values, and accepts only `true` or
  * `false` for known boolean properties; it does not branch on cloud, and no
  * cloud name or scheme (`s3a`, `oss`, `abfs`) appears in it. Dispatch by cloud
  * happens in the C layer, keyed on `fs.cloud_provider`.
  *
  * Translating Hadoop-style keys (`spark.hadoop.fs.s3a.*`) into `fs.*` is not
  * done here. It belongs to whoever produces the configuration, or to a layer-3
  * shim with a written exit condition.
  *
  * Main types: StorageProperties. Capabilities: R3, G4 (see
  * docs/design/capabilities.md). Design:
  * docs/design/architecture/storage-access.html section 3 and
  * storage-auth.html.
  */
package object credential
