package com.zilliztech.spark.l0data;

import java.util.ArrayList;
import java.util.List;

/**
 * StringPrimaryKeys implements PrimaryKeys for string keys.
 */
public class StringPrimaryKeys implements PrimaryKeys {
    private final List<String> values;
    private long sizeInBytes;
    
    /**
     * Creates a new StringPrimaryKeys with the default capacity.
     */
    public StringPrimaryKeys() {
        this(10);
    }
    
    /**
     * Creates a new StringPrimaryKeys with the specified capacity.
     *
     * @param capacity The initial capacity
     */
    public StringPrimaryKeys(int capacity) {
        this.values = new ArrayList<>(capacity);
        this.sizeInBytes = 0;
    }
    
    /**
     * Adds raw string values to this collection.
     *
     * @param values The values to add
     */
    public void appendRaw(String... values) {
        for (String value : values) {
            this.values.add(value);
            this.sizeInBytes += (value.length() * 2L) + 16L; // approximation of string size
        }
    }
    
    @Override
    public void append(PrimaryKey... pks) throws IllegalArgumentException {
        for (PrimaryKey pk : pks) {
            if (!(pk instanceof StringPrimaryKey)) {
                throw new IllegalArgumentException("Expected StringPrimaryKey but got " + pk.getClass().getName());
            }
            String value = (String) pk.getValue();
            values.add(value);
            sizeInBytes += (value.length() * 2L) + 16L; // approximation of string size
        }
    }
    
    @Override
    public PrimaryKey get(int index) throws IndexOutOfBoundsException {
        return new StringPrimaryKey(values.get(index));
    }
    
    @Override
    public DataType getType() {
        return DataType.STRING;
    }
    
    @Override
    public long getSizeInBytes() {
        return sizeInBytes;
    }
    
    @Override
    public int size() {
        return values.size();
    }
    
    @Override
    public void merge(PrimaryKeys pks) throws IllegalArgumentException {
        if (!(pks instanceof StringPrimaryKeys)) {
            throw new IllegalArgumentException("Cannot merge different types of PrimaryKeys");
        }
        
        StringPrimaryKeys other = (StringPrimaryKeys) pks;
        for (int i = 0; i < other.size(); i++) {
            String value = (String) other.get(i).getValue();
            values.add(value);
            sizeInBytes += (value.length() * 2L) + 16L;
        }
    }
    
    /**
     * Gets all values as a list.
     *
     * @return The list of values
     */
    public List<String> getValues() {
        return new ArrayList<>(values);
    }
} 