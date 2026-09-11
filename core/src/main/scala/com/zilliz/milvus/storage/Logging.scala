package com.zilliz.milvus.storage

import org.slf4j.{Logger, LoggerFactory}

/** core 的日志门面。
  *
  * 方法名与 org.apache.spark.internal.Logging 一致，第 2 层的文件从 1.x 搬下来 时只换
  * import。消息按名传，级别关掉时不拼字符串。
  *
  * slf4j-api 标 provided：运行时用 Spark 自带的那份，assembly 也会把它排除掉， 否则
  * userClassPathFirst 下会加载两份 binding。
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
