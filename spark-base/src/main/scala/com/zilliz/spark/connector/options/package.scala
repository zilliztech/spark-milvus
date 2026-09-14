package com.zilliz.spark.connector

/** Option names, aliases and validation; registration of the compat
  * implementations into core.
  *
  * `ReadMode` is the one decision of which source a read takes its segments
  * from. `StorageOptions` turns the `fs.*` options into bucket, Hadoop
  * configuration and the driver's `ObjectStore`, with `HadoopStorageConfig`
  * translating the other way, Hadoop's `fs.s3a.*` keys into `fs.*`;
  * `BackupSelection` picks the collection of a backup export. All are used
  * by the table and by the scan.
  *
  * Capabilities: W6, G1, G2, G3, G4 (see docs/design/capabilities.md).
  */
package object options
