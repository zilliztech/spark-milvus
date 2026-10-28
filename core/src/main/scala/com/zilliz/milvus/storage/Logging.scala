package com.zilliz.milvus.storage

import org.slf4j.{Logger, LoggerFactory}

/** The logging facade for the core layer.
  *
  * The method names match org.apache.spark.internal.Logging, so a file moving
  * down from 1.x only has to change its import. Messages are by-name, so
  * nothing is concatenated when the level is off.
  *
  * slf4j-api is declared provided: at runtime the copy Spark ships is used, and
  * assembly filters it out of the fat jar. Bundling it would load two bindings
  * under spark.executor.userClassPathFirst=true.
  */
trait Logging {

  @transient
  private lazy val logger: Logger =
    LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  protected def logTrace(message: => String): Unit =
    if (logger.isTraceEnabled) logger.trace(message)

  protected def logDebug(message: => String): Unit =
    if (logger.isDebugEnabled) logger.debug(message)

  protected def logInfo(message: => String): Unit =
    if (logger.isInfoEnabled) logger.info(message)

  protected def logWarning(message: => String): Unit =
    if (logger.isWarnEnabled) logger.warn(message)

  protected def logWarning(message: => String, throwable: Throwable): Unit =
    if (logger.isWarnEnabled) logger.warn(message, throwable)

  protected def logError(message: => String): Unit =
    if (logger.isErrorEnabled) logger.error(message)

  protected def logError(message: => String, throwable: Throwable): Unit =
    if (logger.isErrorEnabled) logger.error(message, throwable)
}
