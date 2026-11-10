package com.zilliz.milvus.storage

/** Storage V3 column-file writing, manifest transactions and Arrow resource
  * ownership. Written column groups can cross a process boundary as plain
  * descriptions and be rebuilt for the commit, and a first-revision commit
  * claims an empty path exactly once. These storage primitives contain no Spark
  * or service control; callers own job completion and registration.
  */
package object write
