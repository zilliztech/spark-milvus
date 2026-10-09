package com.zilliz.milvus.storage.codec

import io.milvus.grpc.schema.{
  BoolArray,
  DataType,
  DoubleArray,
  FloatArray,
  IntArray,
  LongArray,
  ScalarField,
  StringArray
}

/** One value of a Milvus `Array` field, as it is stored.
  *
  * Milvus writes an Array column as Arrow `Binary` and each value as a
  * serialized protobuf `ScalarField` holding the elements
  * (`AddOneArrayToPayload` in `internal/storage/payload_writer.go`). The
  * element type is the field's `element_type`; the `ScalarField` carries the
  * same information in which `data` member is set.
  */
object ArrayCodec {

  def decode(bytes: Array[Byte]): ScalarField = ScalarField.parseFrom(bytes)

  /** The elements as boxed JVM values: Boolean, Int (Int8, Int16 and Int32 all
    * travel as Int), Long, Float, Double or String. An empty array is an empty
    * sequence.
    */
  def elements(field: ScalarField): Seq[Any] = field.data match {
    case ScalarField.Data.BoolData(a)   => a.data
    case ScalarField.Data.IntData(a)    => a.data
    case ScalarField.Data.LongData(a)   => a.data
    case ScalarField.Data.FloatData(a)  => a.data
    case ScalarField.Data.DoubleData(a) => a.data
    case ScalarField.Data.StringData(a) => a.data
    case ScalarField.Data.Empty         => Seq.empty
    case other =>
      throw new IllegalArgumentException(
        s"an Array value holds ${other.getClass.getSimpleName}, which is not an element type"
      )
  }

  def elements(bytes: Array[Byte]): Seq[Any] = elements(decode(bytes))

  /** One Array value as Milvus stores it: the `ScalarField` member for
    * `elementType` holding `values`, serialized. The member is set even for an
    * empty array, so the value is never zero bytes; segcore reads a zero-length
    * Binary value as null (FieldData.cpp).
    *
    * An Int8 or Int16 element has to be within its type's range, as the Milvus
    * proxy checks on insert (internal/proxy/fieldvalidator/validate_util.go
    * verifyOverflowByRange): the three integer types share `IntData`, so only
    * the element type tells their ranges apart.
    *
    * @param values
    *   Boolean, Int, Long, Float, Double or String, as [[elements]] returns
    *   them; Int8 and Int16 elements are Ints.
    */
  def encode(elementType: DataType, values: Seq[Any]): Array[Byte] = {
    def as[T](name: String)(f: PartialFunction[Any, T]): Seq[T] =
      values.map { v =>
        f.applyOrElse(
          v,
          (other: Any) =>
            throw new IllegalArgumentException(
              s"an Array<$elementType> element must be $name, got ${Option(other).map(_.getClass.getSimpleName).orNull}"
            )
        )
      }
    val data = elementType match {
      case DataType.Bool =>
        ScalarField.Data.BoolData(
          BoolArray(as[Boolean]("a Boolean") { case b: Boolean => b })
        )
      case DataType.Int8 | DataType.Int16 | DataType.Int32 =>
        val ints = as[Int]("an Int") { case i: Int => i }
        val range = elementType match {
          case DataType.Int8 => Some((Byte.MinValue.toInt, Byte.MaxValue.toInt))
          case DataType.Int16 =>
            Some((Short.MinValue.toInt, Short.MaxValue.toInt))
          case _ => None
        }
        range.foreach { case (low, high) =>
          ints.find(i => i < low || i > high).foreach { bad =>
            throw new IllegalArgumentException(
              s"an Array<$elementType> element $bad is outside [$low, $high]"
            )
          }
        }
        ScalarField.Data.IntData(IntArray(ints))
      case DataType.Int64 =>
        ScalarField.Data.LongData(
          LongArray(as[Long]("a Long") { case l: Long => l })
        )
      case DataType.Float =>
        ScalarField.Data.FloatData(
          FloatArray(as[Float]("a Float") { case f: Float => f })
        )
      case DataType.Double =>
        ScalarField.Data.DoubleData(
          DoubleArray(as[Double]("a Double") { case d: Double => d })
        )
      case DataType.VarChar | DataType.String =>
        ScalarField.Data.StringData(
          StringArray(as[String]("a String") { case s: String => s })
        )
      case other =>
        throw new IllegalArgumentException(
          s"$other is not an Array element type"
        )
    }
    ScalarField(data = data).toByteArray
  }
}
