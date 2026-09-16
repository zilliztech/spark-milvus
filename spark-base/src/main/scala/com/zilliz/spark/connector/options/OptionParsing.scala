package com.zilliz.spark.connector.options

import scala.collection.{Map => CollectionMap}

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Strict parsing shared by every user-facing connector option.
  *
  * Numeric values use canonical decimal digits after surrounding whitespace is
  * removed. The helpers report both the option name and the supplied value,
  * including empty, overflowing and non-positive inputs.
  */
private[connector] object OptionParsing {
  private val PositiveDecimal = "[1-9][0-9]*".r
  private val NonNegativeDecimal = "(?:0|[1-9][0-9]*)".r

  def value(
      options: CollectionMap[String, String],
      key: String
  ): Option[String] =
    options.collectFirst {
      case (optionKey, optionValue) if optionKey.equalsIgnoreCase(key) =>
        optionValue
    }

  def value(
      options: CaseInsensitiveStringMap,
      key: String
  ): Option[String] = Option(options.get(key))

  def boolean(
      getOption: String => Option[String],
      key: String,
      defaultValue: => Boolean
  ): Boolean =
    getOption(key).map(raw => Option(raw).getOrElse("").trim) match {
      case None                                       => defaultValue
      case Some(raw) if raw.equalsIgnoreCase("true")  => true
      case Some(raw) if raw.equalsIgnoreCase("false") => false
      case Some(raw) =>
        throw new IllegalArgumentException(
          s"Option '$key' must be 'true' or 'false', got '$raw'"
        )
    }

  def positiveInt(
      getOption: String => Option[String],
      key: String,
      defaultValue: Int
  ): Int =
    getOption(key) match {
      case None => defaultValue
      case Some(raw) =>
        val value = parsePositiveDecimal(raw, key, "integer")
        try value.toInt
        catch {
          case _: NumberFormatException =>
            invalid(key, raw, "a positive integer")
        }
    }

  def positiveLong(
      getOption: String => Option[String],
      key: String,
      defaultValue: Long,
      maximum: Long = Long.MaxValue
  ): Long =
    getOption(key) match {
      case None => defaultValue
      case Some(raw) =>
        val value = parsePositiveDecimal(raw, key, "long")
        val parsed =
          try value.toLong
          catch {
            case _: NumberFormatException =>
              invalid(key, raw, "a positive long")
          }
        if (parsed > maximum) {
          throw new IllegalArgumentException(
            s"Option '$key' must be at most $maximum, got '$raw'"
          )
        }
        parsed
    }

  def nonNegativeLong(raw: String, key: String): Long = {
    val value = Option(raw).getOrElse("").trim
    if (!NonNegativeDecimal.pattern.matcher(value).matches()) {
      invalid(key, raw, "a non-negative long")
    }
    try value.toLong
    catch {
      case _: NumberFormatException =>
        invalid(key, raw, "a non-negative long")
    }
  }

  def nonNegativeLongList(
      getOption: String => Option[String],
      key: String
  ): Seq[Long] =
    getOption(key) match {
      case None => Seq.empty
      case Some(raw) =>
        val supplied = Option(raw).getOrElse("")
        val values = supplied
          .split(",", -1)
          .toSeq
          .map(_.trim)
        if (values.exists(_.isEmpty)) {
          throw new IllegalArgumentException(
            s"Option '$key' must be a comma-separated list of non-negative longs without empty entries, got '$supplied'"
          )
        }
        values.map { value =>
          try nonNegativeLong(value, key)
          catch {
            case error: IllegalArgumentException =>
              throw new IllegalArgumentException(
                s"Option '$key' must contain non-negative longs, got '$value' in '$supplied'",
                error
              )
          }
        }.distinct
    }

  private def parsePositiveDecimal(
      raw: String,
      key: String,
      typeName: String
  ): String = {
    val value = Option(raw).getOrElse("").trim
    if (!PositiveDecimal.pattern.matcher(value).matches()) {
      invalid(key, raw, s"a positive $typeName")
    }
    value
  }

  private def invalid[A](key: String, raw: String, expected: String): A =
    throw new IllegalArgumentException(
      s"Option '$key' must be $expected, got '${Option(raw).getOrElse("")}'"
    )
}
