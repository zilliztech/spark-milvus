package com.zilliztech.spark.l0data;

/**
 * DataType defines the supported data types for primary keys and other values.
 * This enum maps to the Go schemapb.DataType enum.
 */
public enum DataType {
    /**
     * None/unknown data type.
     */
    NONE(0),
    
    /**
     * Boolean data type.
     */
    BOOLEAN(1),
    
    /**
     * Int8 data type.
     */
    INT8(2),
    
    /**
     * Int16 data type.
     */
    INT16(3),
    
    /**
     * Int32 data type.
     */
    INT32(4),
    
    /**
     * Int64 data type.
     */
    INT64(5),
    
    /**
     * Float data type.
     */
    FLOAT(10),
    
    /**
     * Double data type.
     */
    DOUBLE(11),
    
    /**
     * String data type.
     */
    STRING(20),
    
    /**
     * VarChar data type.
     */
    VARCHAR(21),
    
    /**
     * Array data type.
     */
    ARRAY(22),
    
    /**
     * JSON data type.
     */
    JSON(23),
    
    /**
     * Binary vector data type.
     */
    BINARY_VECTOR(100),
    
    /**
     * Float vector data type.
     */
    FLOAT_VECTOR(101);
    
    private final int code;

    DataType(int code) {
        this.code = code;
    }

    /**
     * Gets the numeric code for this data type.
     *
     * @return The data type code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets a DataType from its numeric code.
     *
     * @param code The data type code
     * @return The corresponding DataType
     * @throws IllegalArgumentException if the code is invalid
     */
    public static DataType fromCode(int code) {
        for (DataType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid data type code: " + code);
    }
    
    /**
     * Checks if this data type is a numeric type.
     *
     * @return true if this data type is a numeric type
     */
    public boolean isNumeric() {
        return this == INT8 || this == INT16 || this == INT32 || this == INT64 || this == FLOAT || this == DOUBLE;
    }
    
    /**
     * Checks if this data type is an integer type.
     *
     * @return true if this data type is an integer type
     */
    public boolean isInteger() {
        return this == INT8 || this == INT16 || this == INT32 || this == INT64;
    }
    
    /**
     * Checks if this data type is a floating-point type.
     *
     * @return true if this data type is a floating-point type
     */
    public boolean isFloatingPoint() {
        return this == FLOAT || this == DOUBLE;
    }
    
    /**
     * Checks if this data type is a string type.
     *
     * @return true if this data type is a string type
     */
    public boolean isString() {
        return this == STRING || this == VARCHAR;
    }
    
    /**
     * Checks if this data type is a binary type.
     *
     * @return true if this data type is a binary type
     */
    public boolean isBinary() {
        return this == BINARY_VECTOR;
    }
    
    /**
     * Checks if this data type is a vector type.
     *
     * @return true if this data type is a vector type
     */
    public boolean isVector() {
        return this == BINARY_VECTOR || this == FLOAT_VECTOR;
    }
} 