package com.zilliz.milvus.storage

/** Milvus 列值的字节编码与解码，纯 JVM，读写两侧共用。
  *
  * Float16 与 BFloat16 的拆装、稀疏向量的 (index, value) 编码都在这里。 和 schema 包的分工：schema
  * 回答「这个字段是什么类型」，codec 回答 「这个类型的一个值在字节里长什么样」。
  *
  * 主要类型：FloatConverter、SparseFloatVectorConverter。 承载的功能：R15、W3（见
  * docs/design/capabilities.md）。
  */
package object codec
