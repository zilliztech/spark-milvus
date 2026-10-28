/**
 * Runnable Java examples of the public connector's fixed V3 snapshot reads,
 * filtering, analytics, vector functions, nearest-neighbor searches and metadata
 * selection. Each example shows its connector calls directly; DemoSupport owns
 * command-line setup, Spark lifecycle and bounded verification of sample results.
 * SnapshotPath separately describes existing snapshot metadata through the
 * Milvus REST API and emits storage options without starting a Spark session.
 * These examples consume existing inputs and do not prepare or modify snapshots.
 */
package com.zilliz.spark.connector.demo;
