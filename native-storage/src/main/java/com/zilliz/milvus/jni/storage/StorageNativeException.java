package com.zilliz.milvus.jni.storage;

/** A {@code LoonFFIResult} that was not success. */
public class StorageNativeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int errorCode;

    public StorageNativeException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public int errorCode() {
        return errorCode;
    }
}
