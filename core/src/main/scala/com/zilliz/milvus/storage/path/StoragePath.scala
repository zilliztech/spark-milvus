package com.zilliz.milvus.storage.path

/** 存储路径的归一。
  *
  * 同一个路径在格式里有三种写法：带 scheme 的全限定路径、桶内相对 key、 带桶名但不带 scheme。快照 JSON 的 base_path
  * 两种都出现过。这里把它们收敛成 一种，消费者不必各自判断。
  */
object StoragePath {

  /** 补齐 scheme 和桶名。
    *
    * s3 与 s3a 一律换成调用方指定的 scheme：同一份快照里两种前缀混着出现， 而 Hadoop 按 scheme 选 FileSystem
    * 实现，不统一就会走到没配凭证的那一个。
    */
  def resolvePath(
      path: String,
      bucket: String,
      storageScheme: String = "s3a"
  ): String = {
    val scheme = storageScheme.stripSuffix("://")
    if (path == null) path
    else if (path.startsWith("s3a://"))
      s"$scheme://" + path.stripPrefix("s3a://")
    else if (path.startsWith("s3://"))
      s"$scheme://" + path.stripPrefix("s3://")
    else if (path.contains("://")) path
    else if (bucket != null && bucket.nonEmpty)
      s"$scheme://$bucket/${path.stripPrefix("/")}"
    else path
  }
}
