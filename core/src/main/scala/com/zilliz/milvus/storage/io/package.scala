package com.zilliz.milvus.storage

/** 对象存储读写的最小接口：open、list、exists、stat、create，没有 rename 和 delete。
  *
  * core 源码不出现 org.apache.hadoop，换实现时不改 core 的公开签名。唯一实现在
  * `io.hadoop`，hadoop-common 标 provided，运行时用 Spark 自带的那份。 executor 上拿到的是可序列化的
  * ObjectStoreFactory，不是活的 Configuration。
  *
  * 主要类型：ObjectStore、ObjectStoreFactory、FileInfo、SeekableInput。 承载的功能：R2、R3、W1（见
  * docs/design/capabilities.md）。
  */
package object io
