package com.zilliz.milvus.storage.snapshot.json

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{
  DeserializationFeature,
  JsonNode,
  ObjectMapper
}
import com.fasterxml.jackson.databind.cfg.{CoercionAction, CoercionInputShape}
import com.fasterxml.jackson.module.scala.{
  DefaultScalaModule,
  ScalaObjectMapper
}

/** Values a snapshot JSON writes as either a number or a string: ids, counts
  * and enum codes differ between Milvus versions. The shape types keep the raw
  * `JsonNode` and settle the type through one of these.
  */
object JsonValues {
  def toLong(value: Any): Long = value match {
    case l: Long                          => l
    case i: Int                           => i.toLong
    case n: Number                        => n.longValue()
    case s: String                        => s.toLong
    case node: JsonNode if node.isNumber  => node.asLong()
    case node: JsonNode if node.isTextual => node.asText().toLong
    case _                                => 0L
  }

  def toInt(value: Any): Int = value match {
    case i: Int                           => i
    case l: Long                          => l.toInt
    case n: Number                        => n.intValue()
    case s: String                        => s.toInt
    case node: JsonNode if node.isNumber  => node.asInt()
    case node: JsonNode if node.isTextual => node.asText().toInt
    case _                                => 0
  }

  def toLongSeq(value: Any): Seq[Long] = value match {
    case seq: Seq[_] => seq.map(toLong)
    case node: JsonNode if node.isArray =>
      import scala.collection.JavaConverters._
      node.elements().asScala.map(n => toLong(n)).toSeq
    case _ => Seq.empty
  }

  /** A data type as its numeric code, from either the code (`5`) or the enum
    * name (`"Int64"`).
    */
  def toDataTypeCode(value: Any): Int =
    code(value, FieldJson.dataTypeNameToCode)

  def toFieldStateCode(value: Any): Int =
    code(value, FieldJson.fieldStateNameToCode)

  private def code(value: Any, byName: String => Int): Int = value match {
    case i: Int                           => i
    case l: Long                          => l.toInt
    case n: Number                        => n.intValue()
    case s: String                        => codeOfText(s, byName)
    case node: JsonNode if node.isNumber  => node.asInt()
    case node: JsonNode if node.isTextual => codeOfText(node.asText(), byName)
    case _                                => 0
  }

  private def codeOfText(text: String, byName: String => Int): Int =
    try text.toInt
    catch { case _: NumberFormatException => byName(text) }
}

/** The one Jackson mapper for every document in this package: unknown
  * properties are ignored and numbers written as strings are accepted.
  *
  * Writing uses a second mapper, because a field a shape type left absent means
  * "the source did not say" and must stay out of the document rather than
  * appear as null.
  */
private[json] object Mapper {
  val mapper: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m.configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, false)
    m.coercionConfigFor(classOf[java.lang.Integer])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[java.lang.Long])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[Int])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m.coercionConfigFor(classOf[Long])
      .setCoercion(CoercionInputShape.String, CoercionAction.TryConvert)
    m
  }

  val writer: ObjectMapper with ScalaObjectMapper = {
    val m = new ObjectMapper() with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    m
  }

  def read[A: Manifest](json: String): Either[Throwable, A] =
    try Right(mapper.readValue[A](json))
    catch { case e: Exception => Left(e) }

  def write(value: Any): String = writer.writeValueAsString(value)
}
