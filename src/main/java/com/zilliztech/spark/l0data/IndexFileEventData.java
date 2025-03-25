package com.zilliztech.spark.l0data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * IndexFileEventData represents the data part of an event in the delta log file.
 */
public class IndexFileEventData {
    private final List<Object> pkValues;
    private final List<Long> timestamps;
    private final DataType dataType;
    
    /**
     * Creates a new IndexFileEventData instance.
     *
     * @param dataType The data type
     */
    public IndexFileEventData(DataType dataType) {
        this.pkValues = new ArrayList<>();
        this.timestamps = new ArrayList<>();
        this.dataType = dataType;
    }
    
    /**
     * Creates a new IndexFileEventData instance.
     */
    public IndexFileEventData() {
        this(DataType.INT64);
    }
    
    /**
     * Reads the event data from a byte array.
     *
     * @param bytes The byte array to read from
     */
    public void readFromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        
        // Read number of entries
        int count = buffer.getInt();
        
        // Read each entry
        for (int i = 0; i < count; i++) {
            // Read primary key value
            Object pkValue = readPkValue(buffer);
            pkValues.add(pkValue);
            
            // Read timestamp
            long timestamp = buffer.getLong();
            timestamps.add(timestamp);
        }
    }
    
    /**
     * Reads a primary key value from the buffer based on the data type.
     *
     * @param buffer The buffer to read from
     * @return The primary key value
     */
    private Object readPkValue(ByteBuffer buffer) {
        switch (dataType) {
            case INT64:
                return buffer.getLong();
            case STRING:
            case VARCHAR:
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                return new String(bytes);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
    
    /**
     * Gets the primary key value at the specified index.
     *
     * @param index The index
     * @return The primary key value
     */
    public Object getPkValue(int index) {
        return pkValues.get(index);
    }
    
    /**
     * Gets the timestamp at the specified index.
     *
     * @param index The index
     * @return The timestamp
     */
    public long getTimestamp(int index) {
        return timestamps.get(index);
    }
    
    /**
     * Gets the number of entries in this event data.
     *
     * @return The number of entries
     */
    public int getEntryCount() {
        return pkValues.size();
    }
    
    /**
     * Gets the size of the fixed part of the event data in bytes.
     *
     * @return The size in bytes
     */
    public int getEventDataFixPartSize() {
        int size = 4; // Entry count
        
        for (int i = 0; i < pkValues.size(); i++) {
            // Primary key value size
            if (dataType == DataType.INT64) {
                size += 8;
            } else if (dataType == DataType.STRING || dataType == DataType.VARCHAR) {
                String value = (String) pkValues.get(i);
                size += 4 + value.length(); // Length + string bytes
            }
            
            // Timestamp size
            size += 8;
        }
        
        return size;
    }
} 