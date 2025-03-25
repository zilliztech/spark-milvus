package com.zilliztech.spark.l0data;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * DescriptorEventData contains the data portion of a descriptor event.
 * This class aligns with the Go implementation's descriptorEventData struct.
 */
public class DescriptorEventData {
    private final long collectionID;
    private final long partitionID;
    private final long segmentID;
    private final long fieldID;
    private final long startTimestamp;
    private final long endTimestamp;
    private final int payloadDataType;
    private final byte[] postHeaderLengths;
    private final byte[] extraBytes;
    private final Map<String, Object> extras;

    /**
     * Creates a new DescriptorEventData.
     *
     * @param collectionID The collection ID
     * @param partitionID The partition ID
     * @param segmentID The segment ID
     * @param fieldID The field ID
     * @param startTimestamp The start timestamp
     * @param endTimestamp The end timestamp
     * @param payloadDataType The payload data type
     * @param postHeaderLengths The post header lengths for each event type
     * @param extraBytes The extra bytes containing JSON data
     */
    public DescriptorEventData(long collectionID, long partitionID, long segmentID, long fieldID,
                             long startTimestamp, long endTimestamp, int payloadDataType,
                             byte[] postHeaderLengths, byte[] extraBytes) {
        this.collectionID = collectionID;
        this.partitionID = partitionID;
        this.segmentID = segmentID;
        this.fieldID = fieldID;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.payloadDataType = payloadDataType;
        this.postHeaderLengths = postHeaderLengths;
        this.extraBytes = extraBytes;
        this.extras = parseExtras(extraBytes);
    }

    private Map<String, Object> parseExtras(byte[] extraBytes) {
        if (extraBytes == null || extraBytes.length == 0) {
            return new HashMap<>();
        }

        try {
            String jsonStr = new String(extraBytes, StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> extras = mapper.readValue(jsonStr, Map.class);
            
            // Validate original_size is present and in string format
            Object sizeStored = extras.get("original_size");
            if (sizeStored == null) {
                throw new IllegalArgumentException("original_size not found in extras");
            }
            if (!(sizeStored instanceof String)) {
                throw new IllegalArgumentException("original_size must be in string format");
            }
            try {
                Integer.parseInt((String)sizeStored);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("original_size must be convertible to integer");
            }
            
            return extras;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse extras JSON: " + e.getMessage());
        }
    }

    /**
     * Gets the collection ID.
     *
     * @return The collection ID
     */
    public long getCollectionID() {
        return collectionID;
    }

    /**
     * Gets the partition ID.
     *
     * @return The partition ID
     */
    public long getPartitionID() {
        return partitionID;
    }

    /**
     * Gets the segment ID.
     *
     * @return The segment ID
     */
    public long getSegmentID() {
        return segmentID;
    }

    /**
     * Gets the field ID.
     *
     * @return The field ID
     */
    public long getFieldID() {
        return fieldID;
    }

    /**
     * Gets the start timestamp.
     *
     * @return The start timestamp
     */
    public long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Gets the end timestamp.
     *
     * @return The end timestamp
     */
    public long getEndTimestamp() {
        return endTimestamp;
    }

    /**
     * Gets the payload data type.
     *
     * @return The payload data type code
     */
    public int getPayloadDataType() {
        return payloadDataType;
    }

    /**
     * Gets the post header lengths.
     *
     * @return The post header lengths
     */
    public byte[] getPostHeaderLengths() {
        return postHeaderLengths;
    }

    /**
     * Gets the extra bytes.
     *
     * @return The extra bytes
     */
    public byte[] getExtraBytes() {
        return extraBytes;
    }

    /**
     * Gets the extras map.
     *
     * @return The extras map
     */
    public Map<String, Object> getExtras() {
        return extras;
    }

    /**
     * Gets the data type.
     *
     * @return The data type
     */
    public DataType getDataType() {
        return DataType.fromCode(payloadDataType);
    }

    /**
     * Gets the size of the fixed part in bytes.
     *
     * @return The size in bytes
     */
    public int getEventDataFixPartSize() {
        return 52; // CollectionID(8) + PartitionID(8) + SegmentID(8) + FieldID(8) + StartTimestamp(8) + EndTimestamp(8) + PayloadDataType(4)
    }

    /**
     * Gets the total memory usage in bytes.
     *
     * @return The memory usage in bytes
     */
    public int getMemoryUsageInBytes() {
        return getEventDataFixPartSize() + postHeaderLengths.length + 4 + (extraBytes != null ? extraBytes.length : 0);
    }
}