package com.zilliztech.spark.l0data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * EventHeader represents the header of an event in the delta log file.
 * This class aligns with the Go implementation's baseEventHeader struct.
 */
public class EventHeader {
    // Size of event header in bytes:
    // timestamp (8) + typeCode (1) + eventLength (4) + nextPosition (4) = 17 bytes
    public static final int EVENT_SIZE = 17;
    
    private long timestamp;
    private EventType eventType;
    private int eventLength;
    private int nextPosition;
    
    /**
     * Creates a new EventHeader with default values.
     */
    public EventHeader() {
        this.timestamp = 0;
        this.eventType = EventType.DESCRIPTOR_EVENT;
        this.eventLength = -1;
        this.nextPosition = -1;
    }
    
    /**
     * Gets the timestamp.
     *
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
    
    /**
     * Gets the event type.
     *
     * @return The event type
     */
    public EventType getEventType() {
        return eventType;
    }
    
    /**
     * Gets the event length.
     *
     * @return The event length
     */
    public int getEventLength() {
        return eventLength;
    }
    
    /**
     * Gets the next position.
     *
     * @return The next position
     */
    public int getNextPosition() {
        return nextPosition;
    }
    
    /**
     * Gets the memory usage in bytes.
     *
     * @return The memory usage in bytes
     */
    public int getMemoryUsageInBytes() {
        return EVENT_SIZE;
    }
    
    /**
     * Reads the header from a byte buffer.
     *
     * @param buffer The buffer to read from
     */
    public void read(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        this.timestamp = buffer.getLong();
        this.eventType = EventType.fromCode(buffer.get() & 0xFF);
        this.eventLength = buffer.getInt();
        this.nextPosition = buffer.getInt();
    }
    
    /**
     * Writes the header to a byte buffer.
     *
     * @param buffer The buffer to write to
     */
    public void write(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(timestamp);
        buffer.put((byte) eventType.getCode());
        buffer.putInt(eventLength);
        buffer.putInt(nextPosition);
    }
} 