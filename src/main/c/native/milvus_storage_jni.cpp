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
    ReadProperties *props = new ReadProperties();
    read_properties_default(props);
    return (jlong)props;
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

    ReadProperties *props = new ReadProperties();
    int result = read_properties_create((const char**)keys_array, (const char**)values_array, keys_length, props);

    free_cstring_array(keys_array, keys_length);
    free_cstring_array(values_array, values_length);

    if (result != 0) {
        delete props;
        return 0;
    }

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
    ReadProperties *props = (ReadProperties*)properties;
    read_properties_free(props);
    delete props;
}

// ChunkReader methods - simplified implementations
JNIEXPORT jlongArray JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunkIndices
(JNIEnv *env, jclass, jlong reader, jlongArray rowIndices) {
    // Return empty array for now - implementation requires proper error handling
    return env->NewLongArray(0);
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunk
(JNIEnv *env, jclass, jlong reader, jlong chunkIndex) {
    // Return 0 for now - implementation requires proper ArrowArray handling
    return 0;
}

JNIEXPORT jlongArray JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunks
(JNIEnv *env, jclass, jlong reader, jlongArray chunkIndices, jlong parallelism) {
    // Return empty array for now - implementation requires proper ArrowArray handling
    return env->NewLongArray(0);
}

JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_chunkReaderDestroy
(JNIEnv *env, jclass, jlong reader) {
    // Simple destroy call - assuming function exists
    if (reader != 0) {
        // chunk_reader_destroy((ChunkReaderHandle)reader);
    }
}

// Reader methods
JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readerNew
(JNIEnv *env, jclass, jlong fs, jstring manifest, jlong schema, jobjectArray neededColumns, jlong properties) {
    char *manifest_cstr = jstring_to_cstring(env, manifest);
    if (manifest_cstr == nullptr) return 0;

    jsize columns_length;
    char **columns_array = jstringArray_to_cstringArray(env, neededColumns, columns_length);

    ReaderHandle reader_handle = nullptr;
    // Call the actual C API function (signature needs to be checked)
    // For now, return 0 to allow compilation

    delete[] manifest_cstr;
    free_cstring_array(columns_array, columns_length);

    return (jlong)reader_handle;
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getRecordBatchReader
(JNIEnv *env, jclass, jlong reader, jstring predicate, jlong batchSize, jlong bufferSize) {
    char *predicate_cstr = jstring_to_cstring(env, predicate);

    // Call the actual C API function
    // ArrowArrayStream *stream = get_record_batch_reader((ReaderHandle)reader, predicate_cstr,
    //                                                   (uint64_t)batchSize, (uint64_t)bufferSize);

    if (predicate_cstr) delete[] predicate_cstr;

    return 0; // Return 0 for now to allow compilation
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getChunkReader
(JNIEnv *env, jclass, jlong reader, jlong columnGroupId) {
    // return (jlong)get_chunk_reader((ReaderHandle)reader, (uint64_t)columnGroupId);
    return 0; // Return 0 for now to allow compilation
}

JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_take
(JNIEnv *env, jclass, jlong reader, jlongArray rowIndices, jlong parallelism) {
    jsize length = env->GetArrayLength(rowIndices);
    jlong *indices = env->GetLongArrayElements(rowIndices, nullptr);

    int64_t *c_indices = new int64_t[length];
    for (jsize i = 0; i < length; i++) {
        c_indices[i] = (int64_t)indices[i];
    }

    // ArrowArray *result = take((ReaderHandle)reader, c_indices, length, (uint64_t)parallelism);

    env->ReleaseLongArrayElements(rowIndices, indices, JNI_ABORT);
    delete[] c_indices;

    return 0; // Return 0 for now to allow compilation
}

JNIEXPORT void JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readerDestroy
(JNIEnv *env, jclass, jlong reader) {
    // reader_destroy((ReaderHandle)reader);
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