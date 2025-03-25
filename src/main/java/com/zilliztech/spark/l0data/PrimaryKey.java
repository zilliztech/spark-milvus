package com.zilliztech.spark.l0data;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * PrimaryKey interface for all primary key types.
 * Equivalent to Go's PrimaryKey interface.
 */
public interface PrimaryKey {
    /**
     * Checks if this primary key is greater than the other key.
     *
     * @param key The key to compare with
     * @return true if this key is greater than the other key
     */
    boolean gt(PrimaryKey key);
    
    /**
     * Checks if this primary key is greater than or equal to the other key.
     *
     * @param key The key to compare with
     * @return true if this key is greater than or equal to the other key
     */
    boolean ge(PrimaryKey key);
    
    /**
     * Checks if this primary key is less than the other key.
     *
     * @param key The key to compare with
     * @return true if this key is less than the other key
     */
    boolean lt(PrimaryKey key);
    
    /**
     * Checks if this primary key is less than or equal to the other key.
     *
     * @param key The key to compare with
     * @return true if this key is less than or equal to the other key
     */
    boolean le(PrimaryKey key);
    
    /**
     * Checks if this primary key is equal to the other key.
     *
     * @param key The key to compare with
     * @return true if this key is equal to the other key
     */
    boolean eq(PrimaryKey key);
    
    /**
     * Sets the value of this primary key.
     *
     * @param value The value to set
     * @return true if successful, false otherwise
     */
    boolean setValue(Object value);
    
    /**
     * Gets the value of this primary key.
     *
     * @return The key value
     */
    Object getValue();
    
    /**
     * Gets the data type of this primary key.
     *
     * @return The data type
     */
    @JsonIgnore
    DataType getType();
    
    /**
     * Gets the size of this primary key in bytes.
     *
     * @return The size in bytes
     */
    @JsonIgnore
    long getSize();
} 