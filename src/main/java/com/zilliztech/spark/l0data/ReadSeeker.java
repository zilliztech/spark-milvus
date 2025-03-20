package com.zilliztech.spark.l0data;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * ReadSeeker interface provides methods for reading and seeking in a file.
 * This interface combines reading and seeking capabilities.
 */
public interface ReadSeeker extends Closeable {
    /**
     * Reads bytes into the specified byte array.
     *
     * @param b The byte array to read into
     * @return The number of bytes read, or -1 if end of stream
     * @throws IOException If an I/O error occurs
     */
    int read(byte[] b) throws IOException;

    /**
     * Seeks to the specified position in the file.
     *
     * @param pos The position to seek to
     * @throws IOException If an I/O error occurs
     */
    void seek(long pos) throws IOException;

    /**
     * Gets the length of the underlying file.
     *
     * @return The length in bytes
     * @throws IOException If an I/O error occurs
     */
    long length() throws IOException;

    /**
     * Reads up to len bytes into b.
     *
     * @param b The byte array to read into
     * @param off The offset in the array
     * @param len The maximum number of bytes to read
     * @return The number of bytes read
     * @throws IOException If an I/O error occurs
     */
    int read(byte[] b, int off, int len) throws IOException;
    
    /**
     * Reads bytes at offset into b.
     *
     * @param b The byte array to read into
     * @param off The offset in the array 
     * @param len The maximum number of bytes to read
     * @param position The position in the file to read from
     * @return The number of bytes read
     * @throws IOException If an I/O error occurs
     */
    int readAt(byte[] b, int off, int len, long position) throws IOException;
    
    /**
     * Gets an InputStream for this ReadSeeker.
     *
     * @return An InputStream
     */
    InputStream getInputStream() throws IOException;
    
    /**
     * Closes this reader, releasing any resources associated with it.
     *
     * @throws IOException If an I/O error occurs
     */
    void close() throws IOException;
    
    /**
     * Constants for whence parameter in seek
     */
    int SEEK_SET = 0; // Seek from the start of file
    int SEEK_CUR = 1; // Seek from current position
    int SEEK_END = 2; // Seek from end of file

    /**
     * Skips the given number of bytes.
     *
     * @param n The number of bytes to skip
     * @throws IOException If an I/O error occurs
     */
    void skip(long n) throws IOException;

    /**
     * Gets the current position in the file.
     *
     * @return The current position
     * @throws IOException If an I/O error occurs
     */
    long position() throws IOException;
} 