// JNI for milvus-storage's write path: the dataset writer, the segment writer
// and the manifest transaction.
//
// Every entry point here is exported from libmilvus-storage, so unlike the
// per-batch reader in segment_reader_jni.cpp nothing is reimplemented and Arrow
// C++ is not needed: Arrow crosses as an address the caller exported.
//
// Two kinds of column-group handle travel through this file and they must not
// be mixed up:
//   - the one columnGroupsCreate returns, built in segment_reader_jni.cpp from
//     a Java layout, released with columnGroupsDestroy;
//   - the one writerClose and segmentWriterClose return, allocated by the C
//     layer, released with nativeColumnGroupsDestroy.
// Only the second kind is what a transaction appends.

#include <jni.h>

#include <string>
#include <vector>

#include "milvus-storage/ffi_c.h"
// The V2 packed writer lives under ffi_internal, but its entry points are
// FFI_EXPORT and libmilvus-storage exports them.
#include "milvus-storage/ffi_internal/v2_packed_writer_c.h"
#include "jni_common.h"


extern "C" {

// ==================== Dataset writer ====================

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerNew(
    JNIEnv* env,
    jclass,
    jstring base_path,
    jlong arrow_schema_address,
    jobject properties) {
  if (arrow_schema_address == 0) {
    ThrowIllegalArgument(env, "arrow schema address must not be zero");
    return 0;
  }
  Utf8 path(env, base_path);
  Properties props;
  if (!props.Build(env, properties)) return 0;

  LoonWriterHandle handle = 0;
  if (!Check(env, loon_writer_new(
                      path.c_str(),
                      reinterpret_cast<struct ArrowSchema*>(arrow_schema_address),
                      props.get(), &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerWrite(
    JNIEnv* env, jclass, jlong handle, jlong arrow_array_address) {
  if (arrow_array_address == 0) {
    ThrowIllegalArgument(env, "arrow array address must not be zero");
    return;
  }
  Check(env, loon_writer_write(
                 static_cast<LoonWriterHandle>(handle),
                 reinterpret_cast<struct ArrowArray*>(arrow_array_address)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerFlush(
    JNIEnv* env, jclass, jlong handle) {
  Check(env, loon_writer_flush(static_cast<LoonWriterHandle>(handle)));
}

// Returns the LoonColumnGroups the write produced, as a handle the caller
// releases with nativeColumnGroupsDestroy. Zero when the call failed.
JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerClose(
    JNIEnv* env,
    jclass,
    jlong handle,
    jobjectArray meta_keys,
    jobjectArray meta_values) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  if (meta_keys != nullptr && meta_values != nullptr) {
    jsize count = env->GetArrayLength(meta_keys);
    if (env->GetArrayLength(meta_values) != count) {
      ThrowIllegalArgument(env, "metadata keys and values must be the same length");
      return 0;
    }
    for (jsize i = 0; i < count; ++i) {
      auto key = static_cast<jstring>(env->GetObjectArrayElement(meta_keys, i));
      auto value = static_cast<jstring>(env->GetObjectArrayElement(meta_values, i));
      const char* key_chars = env->GetStringUTFChars(key, nullptr);
      const char* value_chars = env->GetStringUTFChars(value, nullptr);
      keys.emplace_back(key_chars != nullptr ? key_chars : "");
      values.emplace_back(value_chars != nullptr ? value_chars : "");
      if (key_chars != nullptr) env->ReleaseStringUTFChars(key, key_chars);
      if (value_chars != nullptr) env->ReleaseStringUTFChars(value, value_chars);
    }
  }
  // The C signature takes char** rather than const char**, so the pointers are
  // copied out of the owning strings here; nothing downstream writes to them.
  std::vector<char*> key_ptrs;
  std::vector<char*> value_ptrs;
  key_ptrs.reserve(keys.size());
  value_ptrs.reserve(values.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    key_ptrs.push_back(const_cast<char*>(keys[i].c_str()));
    value_ptrs.push_back(const_cast<char*>(values[i].c_str()));
  }

  LoonColumnGroups* groups = nullptr;
  if (!Check(env, loon_writer_close(static_cast<LoonWriterHandle>(handle),
                                    key_ptrs.empty() ? nullptr : key_ptrs.data(),
                                    value_ptrs.empty() ? nullptr : value_ptrs.data(),
                                    static_cast<uint16_t>(key_ptrs.size()),
                                    &groups))) {
    return 0;
  }
  return reinterpret_cast<jlong>(groups);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_writer_destroy(static_cast<LoonWriterHandle>(handle));
}

// ==================== Segment writer ====================

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterNew(
    JNIEnv* env,
    jclass,
    jlong arrow_schema_address,
    jstring segment_path,
    jobject properties) {
  if (arrow_schema_address == 0) {
    ThrowIllegalArgument(env, "arrow schema address must not be zero");
    return 0;
  }
  Utf8 path(env, segment_path);
  Properties props;
  if (!props.Build(env, properties)) return 0;

  // No LOB columns: TEXT is out of scope, see capabilities.md section 10.
  LoonSegmentWriterConfig config{};
  config.segment_path = path.c_str();
  config.lob_columns = nullptr;
  config.num_lob_columns = 0;

  LoonSegmentWriterHandle handle = 0;
  if (!Check(env, loon_segment_writer_new(
                      reinterpret_cast<struct ArrowSchema*>(arrow_schema_address),
                      &config, props.get(), &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterWrite(
    JNIEnv* env, jclass, jlong handle, jlong arrow_array_address) {
  if (arrow_array_address == 0) {
    ThrowIllegalArgument(env, "arrow array address must not be zero");
    return;
  }
  Check(env,
        loon_segment_writer_write(
            static_cast<LoonSegmentWriterHandle>(handle),
            reinterpret_cast<struct ArrowArray*>(arrow_array_address)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterFlush(
    JNIEnv* env, jclass, jlong handle) {
  Check(env, loon_segment_writer_flush(
                 static_cast<LoonSegmentWriterHandle>(handle)));
}

// Fills a two-element long[]: the column groups handle and the row count. One
// array rather than two calls because close can only be called once.
JNIEXPORT jlongArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterClose(
    JNIEnv* env, jclass, jlong handle) {
  LoonSegmentWriteOutput output{};
  if (!Check(env, loon_segment_writer_close(
                      static_cast<LoonSegmentWriterHandle>(handle), &output))) {
    return nullptr;
  }
  jlong values[2] = {reinterpret_cast<jlong>(output.column_groups),
                     static_cast<jlong>(output.rows_written)};
  jlongArray out = env->NewLongArray(2);
  if (out != nullptr) env->SetLongArrayRegion(out, 0, 2, values);
  return out;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_segment_writer_destroy(static_cast<LoonSegmentWriterHandle>(handle));
}

// ==================== V2 packed writer ====================

// Writes one file per column group at the given paths. groupOffsets and
// groupIndices are the flattened per-group column index lists the C layer
// takes: offsets[g]..offsets[g+1] bounds group g's slice of indices.
JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_packedWriterNew(
    JNIEnv* env,
    jclass,
    jobjectArray paths,
    jintArray group_offsets,
    jintArray group_indices,
    jlong arrow_schema_address,
    jobject properties,
    jlong buffer_size) {
  if (paths == nullptr || group_offsets == nullptr || group_indices == nullptr ||
      arrow_schema_address == 0) {
    ThrowIllegalArgument(env, "paths, offsets, indices and schema are required");
    return 0;
  }
  jsize path_count = env->GetArrayLength(paths);
  std::vector<std::string> owned;
  owned.reserve(path_count);
  for (jsize i = 0; i < path_count; ++i) {
    auto element = static_cast<jstring>(env->GetObjectArrayElement(paths, i));
    const char* chars = env->GetStringUTFChars(element, nullptr);
    owned.emplace_back(chars != nullptr ? chars : "");
    if (chars != nullptr) env->ReleaseStringUTFChars(element, chars);
    env->DeleteLocalRef(element);
  }
  std::vector<const char*> path_ptrs;
  path_ptrs.reserve(owned.size());
  for (const auto& value : owned) path_ptrs.push_back(value.c_str());

  jsize offset_count = env->GetArrayLength(group_offsets);
  jsize index_count = env->GetArrayLength(group_indices);
  std::vector<jint> offsets(offset_count);
  std::vector<jint> indices(index_count);
  if (offset_count > 0) {
    env->GetIntArrayRegion(group_offsets, 0, offset_count, offsets.data());
  }
  if (index_count > 0) {
    env->GetIntArrayRegion(group_indices, 0, index_count, indices.data());
  }

  Properties props;
  if (!props.Build(env, properties)) return 0;

  LoonPackedWriterHandle handle = 0;
  if (!Check(env, loon_packed_writer_new(
                      path_ptrs.data(), static_cast<int32_t>(path_count),
                      reinterpret_cast<const int32_t*>(offsets.data()),
                      reinterpret_cast<const int32_t*>(indices.data()),
                      static_cast<int32_t>(index_count),
                      reinterpret_cast<ArrowSchema*>(arrow_schema_address),
                      props.get(), static_cast<int64_t>(buffer_size),
                      &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_packedWriterWrite(
    JNIEnv* env, jclass, jlong handle, jlong arrow_array_address) {
  if (arrow_array_address == 0) {
    ThrowIllegalArgument(env, "arrow array address must not be zero");
    return;
  }
  Check(env, loon_packed_writer_write(
                 static_cast<LoonPackedWriterHandle>(handle),
                 reinterpret_cast<ArrowArray*>(arrow_array_address)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_packedWriterClose(
    JNIEnv* env, jclass, jlong handle) {
  Check(env,
        loon_packed_writer_close(static_cast<LoonPackedWriterHandle>(handle)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_packedWriterDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_packed_writer_destroy(static_cast<LoonPackedWriterHandle>(handle));
}

// ==================== Transaction ====================

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionBegin(
    JNIEnv* env,
    jclass,
    jstring base_path,
    jobject properties,
    jlong read_version,
    jint resolve_id,
    jint retry_limit) {
  Utf8 path(env, base_path);
  Properties props;
  if (!props.Build(env, properties)) return 0;

  LoonTransactionHandle handle = 0;
  if (!Check(env, loon_transaction_begin(
                      path.c_str(), props.get(),
                      static_cast<int64_t>(read_version),
                      static_cast<int32_t>(resolve_id),
                      static_cast<uint32_t>(retry_limit), &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionCommit(
    JNIEnv* env, jclass, jlong handle) {
  int64_t version = 0;
  if (!Check(env, loon_transaction_commit(
                      static_cast<LoonTransactionHandle>(handle), &version))) {
    return -1;
  }
  return static_cast<jlong>(version);
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionReadVersion(
    JNIEnv* env, jclass, jlong handle) {
  int64_t version = 0;
  if (!Check(env, loon_transaction_get_read_version(
                      static_cast<LoonTransactionHandle>(handle), &version))) {
    return -1;
  }
  return static_cast<jlong>(version);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionDropColumn(
    JNIEnv* env, jclass, jlong handle, jstring column) {
  Utf8 name(env, column);
  Check(env, loon_transaction_drop_column(
                 static_cast<LoonTransactionHandle>(handle), name.c_str()));
}

// Appends every group of a native LoonColumnGroups, which is the shape
// writerClose and segmentWriterClose hand back.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionAppendFiles(
    JNIEnv* env, jclass, jlong handle, jlong native_column_groups) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr) {
    ThrowIllegalArgument(env, "column groups must not be null");
    return;
  }
  Check(env, loon_transaction_append_files(
                 static_cast<LoonTransactionHandle>(handle), groups));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionAddColumnGroup(
    JNIEnv* env, jclass, jlong handle, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return;
  }
  Check(env, loon_transaction_add_column_group(
                 static_cast<LoonTransactionHandle>(handle),
                 &groups->column_group_array[index]));
}

// Adds every group, which is what a backfill commit does: drop the target
// columns, then add the replacements in the same transaction.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionAddColumnGroups(
    JNIEnv* env, jclass, jlong handle, jlong native_column_groups) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr) {
    ThrowIllegalArgument(env, "column groups must not be null");
    return;
  }
  for (uint32_t i = 0; i < groups->num_of_column_groups; ++i) {
    if (!Check(env, loon_transaction_add_column_group(
                        static_cast<LoonTransactionHandle>(handle),
                        &groups->column_group_array[i]))) {
      return;
    }
  }
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionAddDeltaLog(
    JNIEnv* env, jclass, jlong handle, jstring path, jlong num_entries) {
  Utf8 log(env, path);
  Check(env, loon_transaction_add_delta_log(
                 static_cast<LoonTransactionHandle>(handle), log.c_str(),
                 static_cast<int64_t>(num_entries)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_transaction_destroy(static_cast<LoonTransactionHandle>(handle));
}

// ==================== Manifest ====================

// Opens the manifest at base_path and hands back the LoonManifest it holds.
//
// Reading a manifest is a transaction that is begun, queried and dropped; the
// manifest outlives it. Fills a three-element long[]: the manifest handle to
// destroy, the address of the column groups inside it (its first member, which
// is what a reader takes), and the version actually read.
JNIEXPORT jlongArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_manifestOpen(
    JNIEnv* env, jclass, jstring base_path, jobject properties, jlong read_version) {
  Utf8 path(env, base_path);
  Properties props;
  if (!props.Build(env, properties)) return nullptr;

  LoonTransactionHandle transaction = 0;
  if (!Check(env, loon_transaction_begin(
                      path.c_str(), props.get(),
                      static_cast<int64_t>(read_version),
                      LOON_TRANSACTION_RESOLVE_FAIL, 0, &transaction))) {
    return nullptr;
  }

  int64_t version = 0;
  if (!Check(env, loon_transaction_get_read_version(transaction, &version))) {
    loon_transaction_destroy(transaction);
    return nullptr;
  }
  LoonManifest* manifest = nullptr;
  if (!Check(env, loon_transaction_get_manifest(transaction, &manifest))) {
    loon_transaction_destroy(transaction);
    return nullptr;
  }
  loon_transaction_destroy(transaction);

  jlong values[3] = {
      reinterpret_cast<jlong>(manifest),
      manifest != nullptr
          ? reinterpret_cast<jlong>(&manifest->column_groups)
          : 0L,
      static_cast<jlong>(version)};
  jlongArray out = env->NewLongArray(3);
  if (out != nullptr) env->SetLongArrayRegion(out, 0, 3, values);
  return out;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_manifestDestroy(
    JNIEnv*, jclass, jlong manifest) {
  loon_manifest_destroy(reinterpret_cast<LoonManifest*>(manifest));
}

// ==================== Reading back a native LoonColumnGroups ====================

JNIEXPORT jint JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupsCount(
    JNIEnv*, jclass, jlong native_column_groups) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  return groups == nullptr ? 0
                           : static_cast<jint>(groups->num_of_column_groups);
}

JNIEXPORT jobjectArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupFiles(
    JNIEnv* env, jclass, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return nullptr;
  }
  const LoonColumnGroup& group = groups->column_group_array[index];
  jclass string_class = env->FindClass("java/lang/String");
  jobjectArray out =
      env->NewObjectArray(static_cast<jsize>(group.num_of_files), string_class, nullptr);
  if (out == nullptr) return nullptr;
  for (uint32_t i = 0; i < group.num_of_files; ++i) {
    const char* path = group.files[i].path;
    jstring value = env->NewStringUTF(path != nullptr ? path : "");
    env->SetObjectArrayElement(out, static_cast<jsize>(i), value);
    env->DeleteLocalRef(value);
  }
  return out;
}

JNIEXPORT jlongArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupRowCounts(
    JNIEnv* env, jclass, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return nullptr;
  }
  const LoonColumnGroup& group = groups->column_group_array[index];
  std::vector<jlong> counts(group.num_of_files);
  for (uint32_t i = 0; i < group.num_of_files; ++i) {
    counts[i] = static_cast<jlong>(group.files[i].end_index -
                                   group.files[i].start_index);
  }
  jlongArray out = env->NewLongArray(static_cast<jsize>(counts.size()));
  if (out != nullptr && !counts.empty()) {
    env->SetLongArrayRegion(out, 0, static_cast<jsize>(counts.size()),
                            counts.data());
  }
  return out;
}

JNIEXPORT jobjectArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupColumns(
    JNIEnv* env, jclass, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return nullptr;
  }
  const LoonColumnGroup& group = groups->column_group_array[index];
  jclass string_class = env->FindClass("java/lang/String");
  jobjectArray out = env->NewObjectArray(
      static_cast<jsize>(group.num_of_columns), string_class, nullptr);
  if (out == nullptr) return nullptr;
  for (uint32_t i = 0; i < group.num_of_columns; ++i) {
    const char* column = group.columns[i];
    jstring value = env->NewStringUTF(column != nullptr ? column : "");
    env->SetObjectArrayElement(out, static_cast<jsize>(i), value);
    env->DeleteLocalRef(value);
  }
  return out;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupsDestroy(
    JNIEnv*, jclass, jlong native_column_groups) {
  loon_column_groups_destroy(
      reinterpret_cast<LoonColumnGroups*>(native_column_groups));
}

}  // extern "C"
