package com.zilliz.milvus.storage.read.exec

/** One physical column group, already resolved by the input format.
  *
  * Columns use the names in the Arrow schema. Files are in physical row order,
  * and each count describes the corresponding file. This description has no
  * snapshot, storage-version or field-slot semantics and holds no resources.
  */
final case class PhysicalColumnGroup(
    columnNames: Seq[String],
    filePaths: Seq[String],
    fileRowCounts: Seq[Long]
) extends Serializable {
  require(
    columnNames != null && columnNames.nonEmpty &&
      columnNames.forall(name => name != null && name.nonEmpty),
    "a physical column group must name its columns"
  )
  require(
    columnNames.distinct.size == columnNames.size,
    "a physical column group must not repeat a column name"
  )
  require(
    filePaths != null && filePaths.nonEmpty &&
      filePaths.forall(path => path != null && path.nonEmpty),
    "a physical column group must name its files"
  )
  require(
    fileRowCounts != null && fileRowCounts.size == filePaths.size,
    "a physical column group must have one row count per file"
  )
  require(
    fileRowCounts.forall(_ >= 0L),
    "physical file row counts must be nonnegative"
  )
  fileRowCounts.foldLeft(0L) { (total, rows) =>
    require(rows <= Long.MaxValue - total, "physical row count overflows Long")
    total + rows
  }
}
