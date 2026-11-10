package com.zilliz.milvus.storage.write

/** Preserve the operation's failure while attempting every owned resource
  * close.
  */
object WriterResources {
  def closeAll(primary: Throwable, releases: (() => Unit)*): Unit = {
    var failure = primary
    releases.foreach { release =>
      try release()
      catch {
        case error: Throwable =>
          if (failure == null) failure = error
          else if (failure ne error) failure.addSuppressed(error)
      }
    }
    if (primary == null && failure != null) throw failure
  }

  def closing[A](release: => Unit)(operation: => A): A = {
    var primary: Throwable = null
    try operation
    catch {
      case error: Throwable => primary = error; throw error
    } finally closeAll(primary, () => release)
  }

  /** A result is not owned by the caller until closing its producer succeeds.
    */
  def handoff[A <: AutoCloseable](release: => Unit)(operation: => A): A = {
    var result: A = null.asInstanceOf[A]
    try
      closing(release) {
        result = operation
        result
      }
    catch {
      case primary: Throwable =>
        if (result != null) closeAll(primary, () => result.close())
        throw primary
    }
  }
}
