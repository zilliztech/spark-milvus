package com.zilliz.spark.connector

/** Explicit object-storage integration checks over committed, checksum-verified
  * V3 files served by a disposable MinIO fixture. Covers DataSource reads,
  * identity isolation, native byte/range reads, and Hadoop-to-native storage
  * configuration. Default credential-chain checks use fresh JVMs with only
  * generated MinIO credentials and isolated AWS profile files. Input option
  * files describe only this fixture; missing setup fails the suite rather than
  * canceling a test. No Milvus service or shared UAT data is required.
  */
package object sources
