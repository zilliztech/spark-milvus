package com.zilliz.spark.connector

/** Option names, aliases and validation; the mapping and deprecation warnings
  * for 1.x names; registration of the compat implementations into core.
  *
  * `ReadMode` is the one decision of which source a read takes its segments
  * from. `StorageOptions` turns the `fs.*` options into bucket, Hadoop
  * configuration and the driver's `ObjectStore`; `BackupSelection` picks the
  * collection of a backup export. Both are used by the table and by the scan.
  *
  * Capabilities: W6, K4, G1, G2, G3, G4 (see docs/design/capabilities.md).
  */
package object options
