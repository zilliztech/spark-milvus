package com.zilliztech.spark.l0data;

import java.util.ArrayList;
import java.util.List;

/**
 * Int64PrimaryKeys implements PrimaryKeys for 64-bit integer keys.
 */
public class Int64PrimaryKeys implements PrimaryKeys {
    private final List<Long> values;
    
    /**
     * Creates a new Int64PrimaryKeys with the default capacity.
     */
    public Int64PrimaryKeys() {
        this(10);
    }
    
    /**
     * Creates a new Int64PrimaryKeys with the specified capacity.
     *
     * @param capacity The initial capacity
     */
    public Int64PrimaryKeys(int capacity) {
        this.values = new ArrayList<>(capacity);
    }
    
    /**
     * Adds raw long values to this collection.
     *
     * @param values The values to add
     */
    public void appendRaw(long... values) {
        for (long value : values) {
            this.values.add(value);
        }
    }
    
    @Override
    public void append(PrimaryKey... pks) throws IllegalArgumentException {
        for (PrimaryKey pk : pks) {
            if (!(pk instanceof Int64PrimaryKey)) {
                throw new IllegalArgumentException("Expected Int64PrimaryKey but got " + pk.getClass().getName());
            }
            values.add((Long) pk.getValue());
        }
    }
    
    @Override
    public PrimaryKey get(int index) throws IndexOutOfBoundsException {
        return new Int64PrimaryKey(values.get(index));
    }
    
    @Override
    public DataType getType() {
        return DataType.INT64;
    }
    
    @Override
    public long getSizeInBytes() {
        return values.size() * 8L; // 8 bytes per long
    }
    
    @Override
    public int size() {
        return values.size();
    }
    
    @Override
    public void merge(PrimaryKeys pks) throws IllegalArgumentException {
        if (!(pks instanceof Int64PrimaryKeys)) {
            throw new IllegalArgumentException("Cannot merge different types of PrimaryKeys");
        }
        
        Int64PrimaryKeys other = (Int64PrimaryKeys) pks;
        for (int i = 0; i < other.size(); i++) {
            values.add((Long) other.get(i).getValue());
        }
    }
    
    /**
     * Gets all values as a list.
     *
     * @return The list of values
     */
    public List<Long> getValues() {
        return new ArrayList<>(values);
    }
} 