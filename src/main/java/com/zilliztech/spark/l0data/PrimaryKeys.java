package com.zilliztech.spark.l0data;

/**
 * PrimaryKeys is an interface for collections of PrimaryKey objects.
 */
public interface PrimaryKeys {
    /**
     * Appends primary keys to this collection.
     *
     * @param pks The primary keys to append
     * @throws IllegalArgumentException If the key types don't match
     */
    void append(PrimaryKey... pks) throws IllegalArgumentException;
    
    /**
     * Gets a primary key at the specified index.
     *
     * @param index The index
     * @return The primary key at the index
     * @throws IndexOutOfBoundsException If the index is out of range
     */
    PrimaryKey get(int index) throws IndexOutOfBoundsException;
    
    /**
     * Gets the data type of the primary keys.
     *
     * @return The data type
     */
    DataType getType();
    
    /**
     * Gets the size of this collection in bytes.
     *
     * @return The size in bytes
     */
    long getSizeInBytes();
    
    /**
     * Gets the number of primary keys in this collection.
     *
     * @return The number of primary keys
     */
    int size();
    
    /**
     * Merges another PrimaryKeys collection into this one.
     *
     * @param pks The collection to merge
     * @throws IllegalArgumentException If the collections have different key types
     */
    void merge(PrimaryKeys pks) throws IllegalArgumentException;
} 