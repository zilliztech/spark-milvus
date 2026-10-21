package com.zilliz.milvus.storage.write

/** Business writes over the native session owned by core.io. The adapter reuses
  * its normalized keys, filesystem identity and close lifecycle; it introduces
  * no storage factory or credential resolution.
  *
  * Main types: WritableObjectStore, NativeWritableObjectStore. Capabilities: W1
  * (see docs/design/projects/spark-milvus/capabilities.md).
  */
package object io
