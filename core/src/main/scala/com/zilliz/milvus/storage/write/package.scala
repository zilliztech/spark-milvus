package com.zilliz.milvus.storage

/** Storage V3 column-file writing, manifest transactions and Arrow resource
  * ownership. Written column groups can cross a process boundary as plain
  * descriptions and be rebuilt for the commit. A prepared manifest transaction
  * exposes its submission boundary so callers can distinguish staging failures,
  * an unacknowledged commit and a cleanup error after acknowledgement. These
  * storage primitives contain no Spark or service control; callers own job
  * completion, result-address ownership and registration.
  */
package object write
