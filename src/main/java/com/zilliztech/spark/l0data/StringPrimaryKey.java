package com.zilliztech.spark.l0data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * StringPrimaryKey implements PrimaryKey for string keys.
 * This is equivalent to VarCharPrimaryKey in Go.
 */
public class StringPrimaryKey implements PrimaryKey {
    @JsonProperty("pkValue")
    private String value;
    
    /**
     * Creates a new StringPrimaryKey with an empty string.
     */
    public StringPrimaryKey() {
        this("");
    }
    
    /**
     * Creates a new StringPrimaryKey with the specified value.
     *
     * @param value The key value
     */
    public StringPrimaryKey(String value) {
        this.value = value;
    }
    
    @Override
    public boolean gt(PrimaryKey key) {
        if (!(key instanceof StringPrimaryKey)) {
            return false;
        }
        return this.value.compareTo(((StringPrimaryKey) key).value) > 0;
    }
    
    @Override
    public boolean ge(PrimaryKey key) {
        if (!(key instanceof StringPrimaryKey)) {
            return false;
        }
        return this.value.compareTo(((StringPrimaryKey) key).value) >= 0;
    }
    
    @Override
    public boolean lt(PrimaryKey key) {
        if (!(key instanceof StringPrimaryKey)) {
            return false;
        }
        return this.value.compareTo(((StringPrimaryKey) key).value) < 0;
    }
    
    @Override
    public boolean le(PrimaryKey key) {
        if (!(key instanceof StringPrimaryKey)) {
            return false;
        }
        return this.value.compareTo(((StringPrimaryKey) key).value) <= 0;
    }
    
    @Override
    public boolean eq(PrimaryKey key) {
        if (!(key instanceof StringPrimaryKey)) {
            return false;
        }
        return this.value.equals(((StringPrimaryKey) key).value);
    }
    
    @Override
    public boolean setValue(Object value) {
        if (value instanceof String) {
            this.value = (String) value;
            return true;
        }
        return false;
    }
    
    @Override
    public String getValue() {
        return value;
    }
    
    /**
     * Gets the string value of this primary key.
     *
     * @return The string value
     */
    public String getStringValue() {
        return value;
    }
    
    @Override
    public DataType getType() {
        return DataType.STRING;
    }
    
    @Override
    public long getSize() {
        return value.length() * 2 + 8; // Approximate size: char bytes + object overhead
    }
    
    @Override
    public String toString() {
        return value;
    }
} 