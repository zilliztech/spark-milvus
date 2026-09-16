package com.zilliz.spark.connector

/** MilvusCatalog: a read-only TableCatalog that lists Milvus databases and
  * collections and resolves latest, named, or as-of snapshots. Namespace and
  * table mutations are deliberately unsupported. The createTable signature
  * differs between Spark lines, so each line keeps one thin public class.
  *
  * Capabilities: R1, R2, C1 (see docs/design/capabilities.md).
  */
package object catalog
