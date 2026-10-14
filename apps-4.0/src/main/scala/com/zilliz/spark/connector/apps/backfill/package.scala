package com.zilliz.spark.connector.apps

/** BackfillApp and MilvusBackfill coordinate input validation, joins and the
  * run's job manifest. BackfillStorage owns storage paths, scoped Hadoop access
  * and result JSON output. BackfillSegmentWrite distributes merged rows by
  * segment and writes V2/V3 columns. BackfillVectorColumns encodes the input's
  * vector columns for the collection schema before the backfill merge.
  *
  * Capabilities: O1 (see docs/design/capabilities.md).
  */
package object backfill
