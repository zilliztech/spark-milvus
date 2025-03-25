package com.zilliztech.spark.l0data;

/**
 * DescriptorEvent represents a descriptor event in the delta log file.
 * It contains metadata about the delta log file.
 */
public class DescriptorEvent {
    private final DescriptorEventHeader header;
    private final DescriptorEventData data;
    
    /**
     * Creates a new DescriptorEvent.
     *
     * @param header The event header
     * @param data The event data
     */
    public DescriptorEvent(DescriptorEventHeader header, DescriptorEventData data) {
        this.header = header;
        this.data = data;
    }
    
    /**
     * Gets the event header.
     *
     * @return The event header
     */
    public DescriptorEventHeader getHeader() {
        return header;
    }
    
    /**
     * Gets the event data.
     *
     * @return The event data
     */
    public DescriptorEventData getData() {
        return data;
    }
} 