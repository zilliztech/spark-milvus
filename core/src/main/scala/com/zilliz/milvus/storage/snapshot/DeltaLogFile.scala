package com.zilliz.milvus.storage.snapshot

/** One delete file of a segment, as the segment manifest lists it: its
  * `log_id`, its path and the number of entries it holds. Both storage lines
  * list their delete files this way.
  */
case class DeltaLogFile(
    logId: Long,
    logPath: String,
    entriesNum: Long
)
