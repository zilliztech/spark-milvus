package com.zilliztech.spark.l0data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Int64PrimaryKey implements PrimaryKey for 64-bit integer keys.
 */
public class Int64PrimaryKey implements PrimaryKey {
    @JsonProperty("pkValue")
    private long value;
    
    /**
     * Creates a new Int64PrimaryKey with value 0.
     */
    public Int64PrimaryKey() {
        this(0);
    }
    
    /**
     * Creates a new Int64PrimaryKey with the specified value.
     *
     * @param value The key value
     */
    public Int64PrimaryKey(long value) {
        this.value = value;
    }
    
    @Override
    public boolean gt(PrimaryKey key) {
        if (!(key instanceof Int64PrimaryKey)) {
            return false;
        }
        return this.value > ((Int64PrimaryKey) key).value;
    }
    
    @Override
    public boolean ge(PrimaryKey key) {
        if (!(key instanceof Int64PrimaryKey)) {
            return false;
        }
        return this.value >= ((Int64PrimaryKey) key).value;
    }
    
    @Override
    public boolean lt(PrimaryKey key) {
        if (!(key instanceof Int64PrimaryKey)) {
            return false;
        }
        return this.value < ((Int64PrimaryKey) key).value;
    }
    
    @Override
    public boolean le(PrimaryKey key) {
        if (!(key instanceof Int64PrimaryKey)) {
            return false;
        }
        return this.value <= ((Int64PrimaryKey) key).value;
    }
    
    @Override
    public boolean eq(PrimaryKey key) {
        if (!(key instanceof Int64PrimaryKey)) {
            return false;
        }
        return this.value == ((Int64PrimaryKey) key).value;
    }
    
    @Override
    public boolean setValue(Object value) {
        if (value instanceof Long) {
            this.value = (Long) value;
            return true;
        } else if (value instanceof Integer) {
            this.value = ((Integer) value).longValue();
            return true;
        } else if (value instanceof String) {
            try {
                this.value = Long.parseLong((String) value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }
    
    @Override
    public Object getValue() {
        return value;
    }
    
    @Override
    public DataType getType() {
        return DataType.INT64;
    }
    
    @Override
    public long getSize() {
        return 16; // 8 bytes for the value + object overhead
    }
    
    @Override
    public String toString() {
        return Long.toString(value);
    }
} 