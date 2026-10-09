package com.zilliz.spark.connector

/** MilvusCatalog: a TableCatalog that lists Milvus databases and collections,
  * resolves latest, named, or as-of snapshots, and creates or drops collections
  * through Spark table DDL. Namespace mutations and table alter or rename
  * remain unsupported. The createTable signature differs between Spark lines,
  * so each line keeps one thin public class.
  *
  * Capabilities: R1, R2, C1, C2 (see docs/design/capabilities.md).
  */
package object catalog
