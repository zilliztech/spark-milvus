package com.zilliztech.spark.l0data;

/**
 * DescriptorEventHeader represents the header of a descriptor event.
 */
public class DescriptorEventHeader {
    private final int version;
    private final long serverVersion;
    private final long timestamp;
    
    /**
     * Creates a new DescriptorEventHeader.
     *
     * @param version The version number
     * @param serverVersion The server version
     * @param timestamp The timestamp
     */
    public DescriptorEventHeader(int version, long serverVersion, long timestamp) {
        this.version = version;
        this.serverVersion = serverVersion;
        this.timestamp = timestamp;
    }
    
    /**
     * Gets the version number.
     *
     * @return The version number
     */
    public int getVersion() {
        return version;
    }
    
    /**
     * Gets the server version.
     *
     * @return The server version
     */
    public long getServerVersion() {
        return serverVersion;
    }
    
    /**
     * Gets the timestamp.
     *
     * @return The timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }
} 