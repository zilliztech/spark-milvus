package com.zilliz.spark.connector

/** MilvusCatalog: a read-only TableCatalog for one database namespace and the
  * loadTable overloads that resolve latest, named, or as-of snapshots. Listing
  * and DDL are deliberately unsupported. The createTable signature differs
  * between Spark lines, so each line keeps one thin public class.
  *
  * Capabilities: R1, R2 (see docs/design/capabilities.md).
  */
package object catalog
