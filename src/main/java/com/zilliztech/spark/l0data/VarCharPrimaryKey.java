package com.zilliztech.spark.l0data;

/**
 * VarCharPrimaryKey is a compatibility class that extends StringPrimaryKey.
 * Use StringPrimaryKey instead for new code.
 *
 * @deprecated Use StringPrimaryKey instead
 */
@Deprecated
public class VarCharPrimaryKey extends StringPrimaryKey {
    /**
     * Creates a new VarCharPrimaryKey with an empty string.
     */
    public VarCharPrimaryKey() {
        super();
    }
    
    /**
     * Creates a new VarCharPrimaryKey with the specified value.
     *
     * @param value The key value
     */
    public VarCharPrimaryKey(String value) {
        super(value);
    }
    
    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }
} 