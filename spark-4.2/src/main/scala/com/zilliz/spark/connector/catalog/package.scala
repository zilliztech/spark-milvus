package com.zilliz.spark.connector

/** MilvusCatalog: TableCatalog, SupportsNamespaces, and the loadTable overload
  * that resolves a snapshot. The createTable signature differs between Spark
  * lines, which is why this package is per line rather than shared.
  *
  * Capabilities: none until code lands here; the ids this package is planned to
  * carry are in section 11 of docs/design/capabilities.md..
  */
package object catalog
