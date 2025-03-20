package com.zilliztech.spark.l0data;

/**
 * EventType represents different types of events in the delta log file.
 * This enum corresponds to the EventTypeCode in the Go implementation.
 */
public enum EventType {
    DESCRIPTOR_EVENT(0),
    INSERT_EVENT(1),
    DELETE_EVENT(2),
    CREATE_COLLECTION_EVENT(3),
    DROP_COLLECTION_EVENT(4),
    CREATE_PARTITION_EVENT(5),
    DROP_PARTITION_EVENT(6),
    INDEX_FILE_EVENT(7);

    private final int code;

    EventType(int code) {
        this.code = code;
    }

    /**
     * Gets the numeric code for this event type.
     *
     * @return The event type code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets an EventType from its numeric code.
     *
     * @param code The event type code
     * @return The corresponding EventType
     * @throws IllegalArgumentException if the code is invalid
     */
    public static EventType fromCode(int code) {
        for (EventType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid event type code: " + code);
    }
} 