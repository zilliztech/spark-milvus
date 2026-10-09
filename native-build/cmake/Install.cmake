# Install only our engine and JNI targets here. stage.py resolves the complete
# runtime dependency closure from the selected Conan graph and this directory.
set(milvus_native_targets milvus-storage milvus-storage-jni knowhere knowhere_c knowhere_jni)
if(WITH_CARDINAL)
  list(APPEND milvus_native_targets cardinalv1 cardinalv2)
endif()
install(TARGETS ${milvus_native_targets} LIBRARY DESTINATION lib)
