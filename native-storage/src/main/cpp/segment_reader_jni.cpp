// JNI for milvus-storage's segment read path: column groups, the reader, and
// the per-batch RecordBatchReader the JVM consumes.
//
// Split from storage_native_jni.cpp because this file links Arrow C++ and that
// one does not. The reason is the per-batch reader below.
//
// ---------------------------------------------------------------------------
// Interim implementation, with a named end condition.
//
// milvus-storage already has this exact logic: loon_record_batch_reader_new /
// _read_next / _destroy, added by PR #493 specifically for this connector. But
// they are declared in ffi_jni.h without FFI_EXPORT and compiled only into
// libmilvus-storage-jni, the upstream JNI shim this module replaces. The
// library we link, libmilvus-storage, exports only the ArrowArrayStream form
// (loon_get_record_batch_reader), and that form is unusable from the JVM:
// Arrow Java's importer ignores ArrowArray.offset, so once the packed reader
// returns a sliced batch every later batch re-reads from buffer position 0.
//
// So this file rebuilds those three entry points on top of the stream. It goes
// away when upstream exports theirs from libmilvus-storage — requirement 7 in
// docs/design/architecture/storage-access.html. That is the end condition; the
// Arrow C++ dependency below exists only to satisfy it, and Arrow C++ has no
// stable ABI, so the headers here must match the Arrow statically linked into
// libmilvus-storage.
// ---------------------------------------------------------------------------

#include <jni.h>

#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include "milvus-storage/ffi_c.h"
#include "jni_common.h"

namespace {

// An Arrow failure is not a LoonFFIResult, so it gets the same exception with
// the arrow error code the C layer uses for the same class of problem.
void ThrowArrow(JNIEnv* env, const arrow::Status& status) {
  jclass cls = env->FindClass(kExceptionClass);
  if (cls == nullptr) return;
  jmethodID ctor = env->GetMethodID(cls, "<init>", "(ILjava/lang/String;)V");
  jstring message = env->NewStringUTF(status.ToString().c_str());
  jobject exception =
      env->NewObject(cls, ctor, static_cast<jint>(loon_errcode_arrow), message);
  if (exception != nullptr) {
    env->Throw(static_cast<jthrowable>(exception));
  }
}


// Copies a Java String[] into owned std::strings plus a parallel array of
// pointers, which is the shape every loon_* entry point takes.
class StringArray {
 public:
  bool Build(JNIEnv* env, jobjectArray array) {
    if (array == nullptr) return true;
    jsize count = env->GetArrayLength(array);
    values_.reserve(count);
    for (jsize i = 0; i < count; ++i) {
      jstring element = static_cast<jstring>(env->GetObjectArrayElement(array, i));
      if (element == nullptr) {
        values_.emplace_back();
        continue;
      }
      const char* chars = env->GetStringUTFChars(element, nullptr);
      values_.emplace_back(chars != nullptr ? chars : "");
      if (chars != nullptr) env->ReleaseStringUTFChars(element, chars);
      env->DeleteLocalRef(element);
    }
    pointers_.reserve(values_.size());
    for (const auto& value : values_) pointers_.push_back(value.c_str());
    return true;
  }

  const char** data() { return pointers_.empty() ? nullptr : pointers_.data(); }
  size_t size() const { return pointers_.size(); }

 private:
  std::vector<std::string> values_;
  std::vector<const char*> pointers_;
};

// Owns every allocation behind a LoonColumnGroups built from Java arrays. The
// C struct holds borrowed pointers, so this has to outlive the reader.
struct ColumnGroupsHolder {
  std::vector<std::vector<std::string>> columns;
  std::vector<std::vector<std::string>> paths;
  std::vector<std::vector<const char*>> column_ptrs;
  std::vector<std::vector<LoonColumnGroupFile>> files;
  std::vector<LoonColumnGroup> groups;
  std::string format;
  LoonColumnGroups value{};
};

struct RecordBatchReaderHolder {
  std::shared_ptr<arrow::RecordBatchReader> reader;
  // Counters the JVM reads back through recordBatchReaderStats: batches
  // handed over, and how many of their columns were materialized by
  // Concatenate (with the bytes that copy produced). One holder is one Spark
  // task, so nothing here is shared or locked.
  int64_t batches = 0;
  int64_t copies = 0;
  int64_t copied_bytes = 0;
};

// Bytes an array's own buffers occupy; what one Concatenate copy cost.
int64_t BufferBytes(const arrow::Array& array) {
  int64_t total = 0;
  for (const auto& buffer : array.data()->buffers) {
    if (buffer != nullptr) total += buffer->size();
  }
  return total;
}

// loon_take owns the outer array allocation; importing a batch moves its
// release callback, but does not free that outer allocation. On an error this
// also releases every batch that has not yet been imported.
struct TakeOutput {
  ArrowArray* arrays = nullptr;
  size_t count = 0;
  ArrowSchema schema{};

  ~TakeOutput() {
    if (schema.release != nullptr) schema.release(&schema);
    loon_free_chunk_arrays(arrays, count);
  }
};

}  // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_columnGroupsCreate(
    JNIEnv* env,
    jclass,
    jobjectArray columns_per_group,
    jobjectArray files_per_group,
    jobjectArray row_counts_per_group,
    jstring format) {
  if (columns_per_group == nullptr || files_per_group == nullptr ||
      row_counts_per_group == nullptr) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "columns, files and row counts must not be null"));
    return 0;
  }
  jsize group_count = env->GetArrayLength(columns_per_group);
  if (env->GetArrayLength(files_per_group) != group_count ||
      env->GetArrayLength(row_counts_per_group) != group_count) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "columns, files and row counts must have the same "
                        "number of column groups"));
    return 0;
  }

  auto holder = std::make_unique<ColumnGroupsHolder>();
  Utf8 fmt(env, format);
  holder->format = fmt.c_str() != nullptr ? fmt.c_str() : "parquet";
  holder->columns.resize(group_count);
  holder->paths.resize(group_count);
  holder->column_ptrs.resize(group_count);
  holder->files.resize(group_count);
  holder->groups.resize(group_count);

  for (jsize g = 0; g < group_count; ++g) {
    auto columns = static_cast<jobjectArray>(
        env->GetObjectArrayElement(columns_per_group, g));
    auto paths = static_cast<jobjectArray>(
        env->GetObjectArrayElement(files_per_group, g));
    auto counts = static_cast<jlongArray>(
        env->GetObjectArrayElement(row_counts_per_group, g));
    if (columns == nullptr || paths == nullptr || counts == nullptr) {
      ThrowArrow(env, arrow::Status::Invalid("column group ", g, " is null"));
      return 0;
    }

    jsize column_count = env->GetArrayLength(columns);
    for (jsize i = 0; i < column_count; ++i) {
      auto element = static_cast<jstring>(env->GetObjectArrayElement(columns, i));
      const char* chars = env->GetStringUTFChars(element, nullptr);
      holder->columns[g].emplace_back(chars != nullptr ? chars : "");
      if (chars != nullptr) env->ReleaseStringUTFChars(element, chars);
      env->DeleteLocalRef(element);
    }
    for (const auto& column : holder->columns[g]) {
      holder->column_ptrs[g].push_back(column.c_str());
    }

    jsize file_count = env->GetArrayLength(paths);
    if (env->GetArrayLength(counts) != file_count) {
      ThrowArrow(env, arrow::Status::Invalid(
                          "column group ", g, " has ", file_count,
                          " files but ", env->GetArrayLength(counts),
                          " row counts"));
      return 0;
    }
    std::vector<jlong> row_counts(file_count);
    if (file_count > 0) {
      env->GetLongArrayRegion(counts, 0, file_count, row_counts.data());
    }

    // start_index/end_index are the file's OWN zero-based row range, not a
    // cumulative offset within the group: the packed reader intersects them
    // against each file's own row groups, so a cumulative second file gets an
    // empty overlap and its rows vanish with no error. That is
    // milvus-storage#657, and WriterRoundTripTest's multi-file case pins it.
    for (jsize f = 0; f < file_count; ++f) {
      auto element = static_cast<jstring>(env->GetObjectArrayElement(paths, f));
      const char* chars = env->GetStringUTFChars(element, nullptr);
      holder->paths[g].emplace_back(chars != nullptr ? chars : "");
      if (chars != nullptr) env->ReleaseStringUTFChars(element, chars);
      env->DeleteLocalRef(element);
    }
    // Take c_str() only once every path is in place. A path short enough for
    // the small-string optimisation keeps its characters inside the string
    // object, so growing the vector moves them and any pointer taken earlier
    // dangles. The column loop above is split for the same reason.
    for (jsize f = 0; f < file_count; ++f) {
      LoonColumnGroupFile file{};
      file.path = holder->paths[g][f].c_str();
      file.start_index = 0;
      file.end_index = static_cast<int64_t>(row_counts[f]);
      file.property_keys = nullptr;
      file.property_values = nullptr;
      file.num_properties = 0;
      holder->files[g].push_back(file);
    }

    LoonColumnGroup& group = holder->groups[g];
    group.columns = holder->column_ptrs[g].empty()
                        ? nullptr
                        : holder->column_ptrs[g].data();
    group.num_of_columns = static_cast<uint32_t>(holder->column_ptrs[g].size());
    group.format = holder->format.c_str();
    group.files = holder->files[g].empty() ? nullptr : holder->files[g].data();
    group.num_of_files = static_cast<uint32_t>(holder->files[g].size());
  }

  holder->value.column_group_array =
      holder->groups.empty() ? nullptr : holder->groups.data();
  holder->value.num_of_column_groups = static_cast<uint32_t>(holder->groups.size());
  return reinterpret_cast<jlong>(holder.release());
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_columnGroupsDestroy(
    JNIEnv*, jclass, jlong handle) {
  delete reinterpret_cast<ColumnGroupsHolder*>(handle);
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerNew(
    JNIEnv* env,
    jclass,
    jlong column_groups,
    jlong arrow_schema_address,
    jobjectArray needed_columns,
    jobject properties) {
  auto* holder = reinterpret_cast<ColumnGroupsHolder*>(column_groups);
  if (holder == nullptr || arrow_schema_address == 0) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "column groups and arrow schema must not be null"));
    return 0;
  }

  StringArray columns;
  if (!columns.Build(env, needed_columns)) return 0;

  Properties props;
  if (!props.Build(env, properties)) return 0;

  LoonReaderHandle reader = 0;
  if (!Check(env, loon_reader_new(
                      &holder->value,
                      reinterpret_cast<struct ArrowSchema*>(arrow_schema_address),
                      columns.data(), columns.size(), props.get(), &reader))) {
    return 0;
  }
  return static_cast<jlong>(reader);
}

// Opens a reader over column groups the C layer allocated, which is what a
// manifest read and a writer close hand back. Same as readerNew otherwise.
JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerNewNative(
    JNIEnv* env,
    jclass,
    jlong native_column_groups,
    jlong arrow_schema_address,
    jobjectArray needed_columns,
    jobject properties) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || arrow_schema_address == 0) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "column groups and arrow schema must not be null"));
    return 0;
  }
  StringArray columns;
  if (!columns.Build(env, needed_columns)) return 0;
  Properties props;
  if (!props.Build(env, properties)) return 0;

  LoonReaderHandle reader = 0;
  if (!Check(env, loon_reader_new(
                      groups,
                      reinterpret_cast<struct ArrowSchema*>(arrow_schema_address),
                      columns.data(), columns.size(), props.get(), &reader))) {
    return 0;
  }
  return static_cast<jlong>(reader);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerDestroySegment(
    JNIEnv*, jclass, jlong reader) {
  loon_reader_destroy(static_cast<LoonReaderHandle>(reader));
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerTake(
    JNIEnv* env, jclass, jlong reader, jlongArray row_indices,
    jobjectArray needed_columns, jint parallelism) {
  if (reader == 0 || row_indices == nullptr || parallelism <= 0) {
    ThrowIllegalArgument(
        env, "reader and row indices are required; parallelism must be positive");
    return 0;
  }
  const jsize count = env->GetArrayLength(row_indices);
  if (count == 0) {
    ThrowIllegalArgument(env, "row indices must not be empty");
    return 0;
  }

  try {
    std::vector<jlong> java_indices(static_cast<size_t>(count));
    env->GetLongArrayRegion(row_indices, 0, count, java_indices.data());
    if (env->ExceptionCheck()) return 0;
    std::vector<int64_t> indices(java_indices.begin(), java_indices.end());
    for (size_t i = 0; i < indices.size(); ++i) {
      if (indices[i] < 0 || (i > 0 && indices[i] <= indices[i - 1])) {
        ThrowIllegalArgument(
            env, "row indices must be nonnegative, sorted and unique");
        return 0;
      }
    }
    StringArray columns;
    if (!columns.Build(env, needed_columns) || env->ExceptionCheck()) return 0;

    TakeOutput output;
    if (!Check(env, loon_take(
                        static_cast<LoonReaderHandle>(reader), indices.data(),
                        indices.size(), static_cast<size_t>(parallelism),
                        columns.data(), columns.size(), &output.arrays,
                        &output.count, &output.schema))) {
      return 0;
    }
    if (output.arrays == nullptr || output.count == 0 ||
        output.schema.release == nullptr) {
      ThrowArrow(env, arrow::Status::Invalid(
                          "loon_take returned no batches for nonempty indices"));
      return 0;
    }
    auto imported_schema = arrow::ImportSchema(&output.schema);
    if (!imported_schema.ok()) {
      ThrowArrow(env, imported_schema.status());
      return 0;
    }
    auto schema = imported_schema.MoveValueUnsafe();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(output.count);
    int64_t rows = 0;
    for (size_t i = 0; i < output.count; ++i) {
      auto imported = arrow::ImportRecordBatch(&output.arrays[i], schema);
      if (!imported.ok()) {
        ThrowArrow(env, imported.status());
        return 0;
      }
      auto batch = imported.MoveValueUnsafe();
      rows += batch->num_rows();
      batches.push_back(std::move(batch));
    }
    if (rows != static_cast<int64_t>(indices.size())) {
      ThrowArrow(env, arrow::Status::Invalid(
                          "loon_take returned ", rows, " rows for ",
                          indices.size(), " requested indices"));
      return 0;
    }
    auto batch_reader = arrow::RecordBatchReader::Make(std::move(batches), schema);
    if (!batch_reader.ok()) {
      ThrowArrow(env, batch_reader.status());
      return 0;
    }
    auto holder = std::make_unique<RecordBatchReaderHolder>();
    holder->reader = batch_reader.MoveValueUnsafe();
    return reinterpret_cast<jlong>(holder.release());
  } catch (const std::exception& error) {
    ThrowArrow(env, arrow::Status::UnknownError(error.what()));
    return 0;
  } catch (...) {
    ThrowArrow(env, arrow::Status::UnknownError("unexpected failure in loon_take JNI"));
    return 0;
  }
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_recordBatchReaderNew(
    JNIEnv* env, jclass, jlong reader, jstring predicate) {
  Utf8 p(env, predicate);
  ArrowArrayStream stream{};
  if (!Check(env, loon_get_record_batch_reader(
                      static_cast<LoonReaderHandle>(reader), p.c_str(),
                      &stream))) {
    return 0;
  }
  auto imported = arrow::ImportRecordBatchReader(&stream);
  if (!imported.ok()) {
    if (stream.release != nullptr) stream.release(&stream);
    ThrowArrow(env, imported.status());
    return 0;
  }
  auto* holder = new RecordBatchReaderHolder{imported.MoveValueUnsafe()};
  return reinterpret_cast<jlong>(holder);
}

// Fills the caller-allocated ArrowArray/ArrowSchema with the next batch.
// Returns false at EOF, leaving both structs' release fields null.
//
// The offset fix is why this function exists. PackedRecordBatchReader::ReadNext
// can return a batch whose columns carry a non-zero offset: when a chunk is
// larger than min_rows it keeps the remainder via rb->Slice(min_rows). The C
// Data Interface says consumers must honour offset; Arrow Java does not. So a
// sliced column is materialized into a fresh offset-0 array, which copies only
// the slice range. Columns already at offset 0 pass through untouched.
JNIEXPORT jboolean JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_recordBatchReaderReadNext(
    JNIEnv* env, jclass, jlong handle, jlong array_address, jlong schema_address) {
  auto* holder = reinterpret_cast<RecordBatchReaderHolder*>(handle);
  auto* out_array = reinterpret_cast<struct ArrowArray*>(array_address);
  auto* out_schema = reinterpret_cast<struct ArrowSchema*>(schema_address);
  if (holder == nullptr || out_array == nullptr || out_schema == nullptr) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "handle, array and schema must not be null"));
    return JNI_FALSE;
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::Status status = holder->reader->ReadNext(&batch);
  if (!status.ok()) {
    ThrowArrow(env, status);
    return JNI_FALSE;
  }
  if (batch == nullptr) {
    out_array->release = nullptr;
    out_schema->release = nullptr;
    return JNI_FALSE;
  }

  bool sliced = false;
  for (int i = 0; i < batch->num_columns(); ++i) {
    if (batch->column(i)->offset() != 0) {
      sliced = true;
      break;
    }
  }
  if (sliced) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(batch->num_columns());
    for (int i = 0; i < batch->num_columns(); ++i) {
      auto column = batch->column(i);
      if (column->offset() == 0) {
        columns.push_back(column);
        continue;
      }
      auto materialized =
          arrow::Concatenate({column}, arrow::default_memory_pool());
      if (!materialized.ok()) {
        ThrowArrow(env, materialized.status());
        return JNI_FALSE;
      }
      auto copy = materialized.MoveValueUnsafe();
      holder->copies += 1;
      holder->copied_bytes += BufferBytes(*copy);
      columns.push_back(std::move(copy));
    }
    batch = arrow::RecordBatch::Make(batch->schema(), batch->num_rows(), columns);
  }
  holder->batches += 1;

  arrow::Status exported = arrow::ExportRecordBatch(*batch, out_array, out_schema);
  if (!exported.ok()) {
    ThrowArrow(env, exported);
    return JNI_FALSE;
  }
  return JNI_TRUE;
}

// {batches, copies, copied_bytes} so far; see the holder.
JNIEXPORT jlongArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_recordBatchReaderStats(
    JNIEnv* env, jclass, jlong handle) {
  auto* holder = reinterpret_cast<RecordBatchReaderHolder*>(handle);
  jlong values[3] = {0, 0, 0};
  if (holder != nullptr) {
    values[0] = holder->batches;
    values[1] = holder->copies;
    values[2] = holder->copied_bytes;
  }
  jlongArray out = env->NewLongArray(3);
  if (out == nullptr) return nullptr;
  env->SetLongArrayRegion(out, 0, 3, values);
  return out;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_recordBatchReaderDestroy(
    JNIEnv*, jclass, jlong handle) {
  delete reinterpret_cast<RecordBatchReaderHolder*>(handle);
}

}  // extern "C"
