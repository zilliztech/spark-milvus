package com.zilliz.milvus.storage.codec

import io.milvus.grpc.schema.ScalarField

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
}
