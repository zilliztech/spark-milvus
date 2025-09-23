#include <jni.h>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <atomic>
#include <mutex>
#include <thread>
#include <cstring>

// Milvus Storage Reader C API
#include "../../../milvus-storage/cpp/include/milvus-storage/reader_c.h"

// Helper functions for string conversion
static char* jstring_to_cstring(JNIEnv *env, jstring jstr) {
    if (jstr == nullptr) return nullptr;

    const char *utf_chars = env->GetStringUTFChars(jstr, nullptr);
    if (utf_chars == nullptr) return nullptr;

    size_t len = strlen(utf_chars);
    char *cstr = new char[len + 1];
    strcpy(cstr, utf_chars);

    env->ReleaseStringUTFChars(jstr, utf_chars);
    return cstr;
}

static char** jstringArray_to_cstringArray(JNIEnv *env, jobjectArray jarray, jsize &length) {
    if (jarray == nullptr) {
        length = 0;
        return nullptr;
    }

    length = env->GetArrayLength(jarray);
    char **carray = new char*[length];

    for (jsize i = 0; i < length; i++) {
        jstring jstr = (jstring)env->GetObjectArrayElement(jarray, i);
        carray[i] = jstring_to_cstring(env, jstr);
        env->DeleteLocalRef(jstr);
    }

    return carray;
}

static void free_cstring_array(char **carray, jsize length) {
    if (carray == nullptr) return;

    for (jsize i = 0; i < length; i++) {
        delete[] carray[i];
    }
    delete[] carray;
}

// ==================== Reader C API JNI Implementations ====================

// ReadProperties methods
JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readPropertiesDefault
(JNIEnv *env, jclass) {
    return (jlong)read_properties_default();
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readPropertiesCreate
(JNIEnv *env, jclass, jobjectArray keys, jobjectArray values) {
    jsize keys_length, values_length;
    char **keys_array = jstringArray_to_cstringArray(env, keys, keys_length);
    char **values_array = jstringArray_to_cstringArray(env, values, values_length);

    if (keys_length != values_length) {
        free_cstring_array(keys_array, keys_length);
        free_cstring_array(values_array, values_length);
        return 0;
    }

    ReadProperties *props = read_properties_create((const char**)keys_array, (const char**)values_array, keys_length);

    free_cstring_array(keys_array, keys_length);
    free_cstring_array(values_array, values_length);

    return (jlong)props;
}

JNIEXPORT jstring JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readPropertiesGet
(JNIEnv *env, jclass, jlong properties, jstring key) {
    char *key_cstr = jstring_to_cstring(env, key);
    if (key_cstr == nullptr) return nullptr;

    const char *value = read_properties_get((ReadProperties*)properties, key_cstr);
    delete[] key_cstr;

    if (value == nullptr) return nullptr;
    return env->NewStringUTF(value);
}

JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readPropertiesFree
(JNIEnv *env, jclass, jlong properties) {
    read_properties_free((ReadProperties*)properties);
}

// ChunkReader methods
JNIEXPORT jlongArray JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunkIndices
(JNIEnv *env, jclass, jlong reader, jlongArray rowIndices) {
    jsize length = env->GetArrayLength(rowIndices);
    jlong *indices = env->GetLongArrayElements(rowIndices, nullptr);

    uint64_t *c_indices = new uint64_t[length];
    for (jsize i = 0; i < length; i++) {
        c_indices[i] = (uint64_t)indices[i];
    }

    size_t result_length;
    uint64_t *result_indices = get_chunk_indices((ChunkReader*)reader, c_indices, length, &result_length);

    env->ReleaseLongArrayElements(rowIndices, indices, JNI_ABORT);
    delete[] c_indices;

    if (result_indices == nullptr) return nullptr;

    jlongArray result = env->NewLongArray(result_length);
    jlong *result_data = new jlong[result_length];
    for (size_t i = 0; i < result_length; i++) {
        result_data[i] = (jlong)result_indices[i];
    }
    env->SetLongArrayRegion(result, 0, result_length, result_data);

    delete[] result_data;
    // Note: result_indices should be freed by the C API if needed

    return result;
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunk
(JNIEnv *env, jclass, jlong reader, jlong chunkIndex) {
    return (jlong)get_chunk((ChunkReader*)reader, (uint64_t)chunkIndex);
}

JNIEXPORT jlongArray JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunks
(JNIEnv *env, jclass, jlong reader, jlongArray chunkIndices, jlong parallelism) {
    jsize length = env->GetArrayLength(chunkIndices);
    jlong *indices = env->GetLongArrayElements(chunkIndices, nullptr);

    uint64_t *c_indices = new uint64_t[length];
    for (jsize i = 0; i < length; i++) {
        c_indices[i] = (uint64_t)indices[i];
    }

    size_t result_length;
    ArrowArray **result_arrays = get_chunks((ChunkReader*)reader, c_indices, length, (uint64_t)parallelism, &result_length);

    env->ReleaseLongArrayElements(chunkIndices, indices, JNI_ABORT);
    delete[] c_indices;

    if (result_arrays == nullptr) return nullptr;

    jlongArray result = env->NewLongArray(result_length);
    jlong *result_data = new jlong[result_length];
    for (size_t i = 0; i < result_length; i++) {
        result_data[i] = (jlong)result_arrays[i];
    }
    env->SetLongArrayRegion(result, 0, result_length, result_data);

    delete[] result_data;

    return result;
}

JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_chunkReaderDestroy
(JNIEnv *env, jclass, jlong reader) {
    chunk_reader_destroy((ChunkReader*)reader);
}

// Reader methods
JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readerNew
(JNIEnv *env, jclass, jlong fs, jstring manifest, jlong schema, jobjectArray neededColumns, jlong properties) {
    char *manifest_cstr = jstring_to_cstring(env, manifest);
    if (manifest_cstr == nullptr) return 0;

    jsize columns_length;
    char **columns_array = jstringArray_to_cstringArray(env, neededColumns, columns_length);

    Reader *reader = reader_new((void*)fs, manifest_cstr, (ArrowSchema*)schema,
                               (const char**)columns_array, columns_length, (ReadProperties*)properties);

    delete[] manifest_cstr;
    free_cstring_array(columns_array, columns_length);

    return (jlong)reader;
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getRecordBatchReader
(JNIEnv *env, jclass, jlong reader, jstring predicate, jlong batchSize, jlong bufferSize) {
    char *predicate_cstr = jstring_to_cstring(env, predicate);

    ArrowArrayStream *stream = get_record_batch_reader((Reader*)reader, predicate_cstr,
                                                      (uint64_t)batchSize, (uint64_t)bufferSize);

    if (predicate_cstr) delete[] predicate_cstr;

    return (jlong)stream;
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunkReader
(JNIEnv *env, jclass, jlong reader, jlong columnGroupId) {
    return (jlong)get_chunk_reader((Reader*)reader, (uint64_t)columnGroupId);
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_take
(JNIEnv *env, jclass, jlong reader, jlongArray rowIndices, jlong parallelism) {
    jsize length = env->GetArrayLength(rowIndices);
    jlong *indices = env->GetLongArrayElements(rowIndices, nullptr);

    uint64_t *c_indices = new uint64_t[length];
    for (jsize i = 0; i < length; i++) {
        c_indices[i] = (uint64_t)indices[i];
    }

    ArrowArray *result = take((Reader*)reader, c_indices, length, (uint64_t)parallelism);

    env->ReleaseLongArrayElements(rowIndices, indices, JNI_ABORT);
    delete[] c_indices;

    return (jlong)result;
}

JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readerDestroy
(JNIEnv *env, jclass, jlong reader) {
    reader_destroy((Reader*)reader);
}

// Arrow helper methods
JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_releaseArrowArray
(JNIEnv *env, jclass, jlong array) {
    if (array != 0) {
        ArrowArray *arr = (ArrowArray*)array;
        if (arr->release) {
            arr->release(arr);
        }
    }
}

JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_releaseArrowSchema
(JNIEnv *env, jclass, jlong schema) {
    if (schema != 0) {
        ArrowSchema *sch = (ArrowSchema*)schema;
        if (sch->release) {
            sch->release(sch);
        }
    }
}

JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_releaseArrowArrayStream
(JNIEnv *env, jclass, jlong stream) {
    if (stream != 0) {
        ArrowArrayStream *str = (ArrowArrayStream*)stream;
        if (str->release) {
            str->release(str);
        }
    }
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getArrowArrayLength
(JNIEnv *env, jclass, jlong array) {
    if (array == 0) return 0;
    ArrowArray *arr = (ArrowArray*)array;
    return (jlong)arr->length;
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getArrowArrayNumChildren
(JNIEnv *env, jclass, jlong array) {
    if (array == 0) return 0;
    ArrowArray *arr = (ArrowArray*)array;
    return (jlong)arr->n_children;
}