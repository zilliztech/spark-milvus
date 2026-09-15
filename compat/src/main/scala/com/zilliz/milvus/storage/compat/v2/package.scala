package com.zilliz.milvus.storage.compat

/** `storage_version = 2` segments, which carry no manifest: their column groups
  * are recovered from the parquet footers by `FooterV2SegmentResolver`, the
  * compat implementation of core.snapshot.V2SegmentResolver.
  *
  * Capabilities: K1 (see docs/design/capabilities.md).
  */
package object v2
