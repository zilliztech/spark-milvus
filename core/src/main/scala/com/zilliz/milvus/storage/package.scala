package com.zilliz.milvus

/** The core layer's shared foundation, used by every package below it: the
  * exceptions a storage format raises, the logging facade and the counter for
  * calls across the JNI boundary.
  *
  * DataParseException reports a schema or data value the format could not
  * parse; DataTypeException reports a type combination the mapping does not
  * support. Logging is the facade core uses instead of Spark's, with the same
  * method names; it binds slf4j at runtime to the copy Spark ships. NativeCalls
  * counts one handle owner's native calls and their time, per batch rather than
  * per row.
  *
  * Nothing here knows a format, a cloud or Spark. A type that does belongs in
  * the package that owns that knowledge.
  *
  * Main types: DataParseException, DataTypeException, Logging, NativeCalls.
  */
package object storage
