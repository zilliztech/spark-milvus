# External components install their own targets; the shared driver stages one graph.
install(TARGETS milvus-storage milvus-storage-jni LIBRARY DESTINATION lib)
