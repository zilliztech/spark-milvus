package com.zilliz.milvus.storage

/** Counts the calls one object makes across the JNI boundary and the time they
  * take. One instance per handle owner, never shared, so plain fields suffice.
  *
  * The cost is one `System.nanoTime` pair per call, and calls happen per batch,
  * not per row (docs/design/architecture/storage-io.html section 5.3).
  */
final class NativeCalls {
  private var _calls: Long = 0L
  private var _nanos: Long = 0L

  def calls: Long = _calls
  def nanos: Long = _nanos

  /** Runs `call`, counting it and its wall time whether it returns or throws.
    */
  def timed[T](call: => T): T = {
    _calls += 1
    val start = System.nanoTime()
    try call
    finally _nanos += System.nanoTime() - start
  }
}
