package com.zilliztech.spark.l0data;

import java.util.ArrayList;
import java.util.List;

/**
 * DeltaData holds a collection of primary keys and corresponding timestamps.
 */
public class DeltaData {
    private final PrimaryKeys pks;
    private final List<Long> timestamps;
    private final DataType dataType;
    
    // Stats
    private long deleteRowCount;
    private long memorySize;
    
    /**
     * Creates a new DeltaData instance for the specified data type.
     *
     * @param dataType The data type
     * @param capacity The initial capacity
     */
    public DeltaData(DataType dataType, int capacity) {
        this.dataType = dataType;
        this.timestamps = new ArrayList<>(capacity);
        this.deleteRowCount = 0;
        this.memorySize = 0;
        
        // Create appropriate PrimaryKeys implementation
        switch (dataType) {
            case INT64:
                this.pks = new Int64PrimaryKeys(capacity);
                break;
            case STRING:
            case VARCHAR:
                this.pks = new StringPrimaryKeys(capacity);
                break;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
    
    /**
     * Creates a new DeltaData instance with timestamps but no primary keys yet.
     * The primary keys will be initialized when the data type is known.
     *
     * @param startTimestamp The start timestamp
     * @param endTimestamp The end timestamp
     */
    public DeltaData(long startTimestamp, long endTimestamp) {
        // Use STRING as a default type, but it will be set properly when data is parsed
        this.dataType = DataType.STRING;
        this.timestamps = new ArrayList<>();
        this.deleteRowCount = 0;
        this.memorySize = 0;
        this.pks = new StringPrimaryKeys(0); // Empty primary keys for now
        
        // Store the timestamps
        timestamps.add(startTimestamp);
        timestamps.add(endTimestamp);
    }
    
    /**
     * Appends a primary key and timestamp to this delta data.
     *
     * @param pk The primary key
     * @param timestamp The timestamp
     */
    public void append(PrimaryKey pk, long timestamp) {
        pks.append(pk);
        timestamps.add(timestamp);
        deleteRowCount++;
    }
    
    /**
     * Processes each primary key and timestamp in this delta data.
     *
     * @param processor The processor function
     */
    public void range(DeltaDataProcessor processor) {
        for (int i = 0; i < deleteRowCount; i++) {
            if (!processor.process(pks.get(i), timestamps.get(i))) {
                break;
            }
        }
    }
    
    /**
     * Merges another DeltaData into this one.
     *
     * @param other The other DeltaData
     * @throws IllegalArgumentException If the data types don't match
     */
    public void merge(DeltaData other) throws IllegalArgumentException {
        if (this.dataType != other.dataType) {
            throw new IllegalArgumentException("Cannot merge DeltaData with different data types");
        }
        
        pks.merge(other.pks);
        timestamps.addAll(other.timestamps);
        deleteRowCount += other.deleteRowCount;
    }
    
    /**
     * Gets the data type of this delta data.
     *
     * @return The data type
     */
    public DataType getDataType() {
        return dataType;
    }
    
    /**
     * Gets the number of rows in this delta data.
     *
     * @return The row count
     */
    public long getDeleteRowCount() {
        return deleteRowCount;
    }
    
    /**
     * Gets the memory size of this delta data in bytes.
     *
     * @return The memory size in bytes
     */
    public long getMemorySize() {
        return memorySize;
    }
    
    /**
     * Gets the timestamps associated with this delta data.
     *
     * @return The list of timestamps
     */
    public List<Long> getTimestamps() {
        return new ArrayList<>(timestamps); // Return a copy to prevent modification
    }
    
    /**
     * Functional interface for processing primary keys and timestamps.
     */
    @FunctionalInterface
    public interface DeltaDataProcessor {
        /**
         * Processes a primary key and timestamp.
         *
         * @param pk The primary key
         * @param timestamp The timestamp
         * @return true to continue, false to stop
         */
        boolean process(PrimaryKey pk, long timestamp);
    }
} 