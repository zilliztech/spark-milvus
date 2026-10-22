package com.zilliz.spark.connector

/** Public read/search options and driver-side storage configuration. Retired
  * service, write, backup and unsupported legacy options fail before I/O.
  * `StorageOptions` and `HadoopStorageKeys` translate effective storage aliases
  * and per-bucket identity into the canonical native fs.* properties.
  * `MilvusOption` parses typed read limits, filters and fixed snapshot input;
  * `SearchLimits` and `TaskResources` supply search execution limits.
  *
  * Capabilities: R16, G1, G3 (see design document capabilities.md).
  */
package object options
