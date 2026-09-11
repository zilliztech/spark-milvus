package com.zilliz.spark.connector

/** MilvusCatalog: TableCatalog, SupportsNamespaces, and the loadTable overload
  * that resolves a snapshot. The createTable signature differs between Spark
  * lines, which is why this package is per line rather than shared.
  *
  * Capabilities: R1, R2, C1, C2, A6 (see docs/design/capabilities.md).
  */
package object catalog
