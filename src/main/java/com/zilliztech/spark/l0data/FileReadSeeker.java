package com.zilliztech.spark.l0data;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.OSSObject;

import java.io.*;

/**
 * FileReadSeeker implements the ReadSeeker interface using RandomAccessFile.
 */
public class FileReadSeeker implements ReadSeeker {
    private final RandomAccessFile file;

    /**
     * Creates a new FileReadSeeker for the specified file path.
     *
     * @param filePath The path to the file
     * @throws IOException If an I/O error occurs
     */
    public FileReadSeeker(String filePath) throws IOException {
        this.file = new RandomAccessFile(filePath, "r");
    }

    public FileReadSeeker(String filePath, OSS oss) throws IOException {
        this.file = openOssFile(filePath, oss);
    }

    private RandomAccessFile openOssFile(String ossPath, OSS ossClient) throws IOException {
        String path = ossPath.substring("oss://".length());
        int slashIndex = path.indexOf('/');
        if (slashIndex < 0) throw new IllegalArgumentException("Invalid OSS path");

        String bucket = path.substring(0, slashIndex);
        String key = path.substring(slashIndex + 1);

        File temp = File.createTempFile("oss-", ".tmp");
        temp.deleteOnExit();

        try (OSSObject ossObject = ossClient.getObject(bucket, key);
             InputStream in = ossObject.getObjectContent();
             FileOutputStream out = new FileOutputStream(temp)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        } finally {
            ossClient.shutdown();
        }

        return new RandomAccessFile(temp, "r");
    }

    /**
     * Creates a new FileReadSeeker for the specified file.
     *
     * @param file The file
     * @throws IOException If an I/O error occurs
     */
    public FileReadSeeker(File file) throws IOException {
        this.file = new RandomAccessFile(file, "r");
    }

    @Override
    public int read(byte[] b) throws IOException {
        return file.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return file.read(b, off, len);
    }

    @Override
    public int readAt(byte[] b, int off, int len, long position) throws IOException {
        long oldPosition = file.getFilePointer();
        try {
            file.seek(position);
            return file.read(b, off, len);
        } finally {
            file.seek(oldPosition);
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        file.seek(pos);
    }

    @Override
    public void skip(long n) throws IOException {
        file.skipBytes((int) n);
    }

    @Override
    public long position() throws IOException {
        return file.getFilePointer();
    }

    @Override
    public long length() throws IOException {
        return file.length();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                return file.read();
            }

            @Override
            public int read(byte[] b) throws IOException {
                return file.read(b);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return file.read(b, off, len);
            }

            @Override
            public long skip(long n) throws IOException {
                return file.skipBytes((int) n);
            }

            @Override
            public void close() throws IOException {
                file.close();
            }
        };
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
} 