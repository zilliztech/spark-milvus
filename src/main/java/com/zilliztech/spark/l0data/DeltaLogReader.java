package com.zilliztech.spark.l0data;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DeltaLogReader is responsible for reading and parsing delta log files.
 * 
 * Delta log files contain events that record changes to a collection, such as:
 * - Insert events: Adding new data
 * - Delete events: Removing data
 * - Index file events: Changes to indices
 * 
 * Each event consists of:
 * 1. A header: Contains metadata about the event (timestamp, type, length, etc.)
 * 2. Data: The actual payload of the event (varies by event type)
 * 
 * The file format follows this structure:
 * - Magic number (4 bytes): Identifies the file as a delta log (0xfffabc)
 * - Descriptor event: Contains metadata about the file (collection ID, data type, etc.)
 * - Series of events: Each with its own header and data
 * 
 * This implementation aligns with the Go implementation in the Milvus codebase.
 */
public class DeltaLogReader implements AutoCloseable, Iterable<DeltaData> {
    private static final Logger logger = LoggerFactory.getLogger(DeltaLogReader.class);
    
    // Magic number from Go implementation: 0xfffabc
    // This is the identifier for delta log files
    private static final int MAGIC_NUMBER = 0xFFFABC;

    // The underlying reader for the delta log file
    private final ReadSeeker reader;

    // Metadata about the file from the descriptor event
    private DescriptorEvent descriptorEvent;
    
    // List of delta data entries read from the file
    private final List<DeltaData> deltaDataList;
    
    // Data type of the primary keys in the file
    private DataType dataType;
    
    /**
     * Creates a new DeltaLogReader for the specified file.
     *
     * @param filePath The path to the delta log file
     * @throws IOException If an I/O error occurs
     */
    public DeltaLogReader(String filePath) throws IOException {
        this(new FileReadSeeker(filePath));
    }
    
    /**
     * Creates a new DeltaLogReader using the specified ReadSeeker.
     * This constructor aligns with the Go implementation's NewBinlogReader function.
     *
     * @param reader The ReadSeeker to use
     * @throws IOException If an I/O error occurs
     */
    public DeltaLogReader(ReadSeeker reader) throws IOException {
        this.reader = reader;
        this.deltaDataList = new ArrayList<>();
        
        // First, read magic number (like Go implementation)
        int magicNumber = readMagicNumber();
        
        // Then read descriptor event (like Go implementation)
        this.descriptorEvent = readDescriptorEvent();
        
        // Get data type from descriptor event
        this.dataType = DataType.fromCode(descriptorEvent.getData().getPayloadDataType());
    }

    /**
     * Gets the descriptor event.
     * 
     * @return The descriptor event
     */
    public DescriptorEvent getDescriptorEvent() {
        return descriptorEvent;
    }
    
    /**
     * Reads the magic number from the file.
     * This implementation aligns with the Go implementation's readMagicNumber function.
     * 
     * @return The magic number read from the file
     * @throws IOException If an I/O error occurs
     */
    private int readMagicNumber() throws IOException {
        // Allocate buffer for magic number (4 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        int bytesRead = reader.read(buffer.array());
        if (bytesRead != 4) {
            throw new IOException(String.format(
                "Failed to read magic number: expected 4 bytes, got %d bytes", 
                bytesRead));
        }
        
        // Read magic number as int32 in little-endian order (like Go implementation)
        int magicNumber = buffer.getInt(0);
        
        // Check magic number
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(String.format(
                "Invalid magic number: expected 0x%06x, got 0x%06x", 
                MAGIC_NUMBER, magicNumber));
        }
        
        return magicNumber;
    }
    
    /**
     * Reads the descriptor event from the file.
     * This implementation aligns with the Go implementation's ReadDescriptorEvent function.
     *
     * @return The descriptor event
     * @throws IOException If an I/O error occurs
     */
    private DescriptorEvent readDescriptorEvent() throws IOException {
        // Read event header (baseEventHeader in Go)
        ByteBuffer headerBuffer = ByteBuffer.allocate(EventHeader.EVENT_SIZE);
        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        
        int bytesRead = reader.read(headerBuffer.array());
        if (bytesRead != EventHeader.EVENT_SIZE) {
            throw new IOException(String.format(
                "Failed to read event header: expected %d bytes, got %d bytes",
                EventHeader.EVENT_SIZE, bytesRead));
        }
        
        // Read header fields manually to ensure correct byte order
        long timestamp = headerBuffer.getLong(0);
        int eventTypeByte = headerBuffer.get(8) & 0xFF; // Read as unsigned byte
        int eventLength = headerBuffer.getInt(9);
        int nextPosition = headerBuffer.getInt(13);
        
        // Verify this is a descriptor event
        if (eventTypeByte != EventType.DESCRIPTOR_EVENT.getCode()) {
            throw new IOException(String.format(
                "Expected descriptor event (type code: %d), got type code: %d",
                EventType.DESCRIPTOR_EVENT.getCode(), eventTypeByte));
        }
        
        // Create event header
        EventHeader header = new EventHeader();
        header.read(headerBuffer);
        
        // Create a placeholder descriptor event header
        DescriptorEventHeader descHeader = new DescriptorEventHeader(1, 0, 0);
        
        // Read descriptor event data - this is the method with the issue
        DescriptorEventData data = readDescriptorEventData();
        
        return new DescriptorEvent(descHeader, data);
    }
    
    /**
     * Reads the descriptor event data from the file.
     * This implementation aligns with the Go implementation's readDescriptorEventData function.
     * 
     * @return The descriptor event data
     * @throws IOException If an I/O error occurs
     */
    private DescriptorEventData readDescriptorEventData() throws IOException {
        // First, reset the reader position to after the header, in case something went wrong
        long currentPosition = reader.position();
        
        // In Go implementation, the following structure is read:
        // DescriptorEventDataFixPart = struct {
        //     CollectionID    int64
        //     PartitionID     int64
        //     SegmentID       int64
        //     FieldID         int64
        //     StartTimestamp  uint64
        //     EndTimestamp    uint64
        //     PayloadDataType int32
        // }
        
        // Read DescriptorEventDataFixPart (all 8 bytes except PayloadDataType which is 4 bytes)
        // Total: 8*6 + 4 = 52 bytes
        ByteBuffer fixPartBuffer = ByteBuffer.allocate(52);
        fixPartBuffer.order(ByteOrder.LITTLE_ENDIAN);
        int bytesRead = reader.read(fixPartBuffer.array());
        
        if (bytesRead != 52) {
            throw new IOException(String.format(
                "Failed to read descriptor event data fixed part: expected 52 bytes, got %d bytes",
                bytesRead));
        }
        
        // Reset position to start
        fixPartBuffer.position(0);
        
        // Read all fields
        long collectionID = fixPartBuffer.getLong();
        long partitionID = fixPartBuffer.getLong();
        long segmentID = fixPartBuffer.getLong();
        long fieldID = fixPartBuffer.getLong();
        long startTimestamp = fixPartBuffer.getLong();
        long endTimestamp = fixPartBuffer.getLong();
        int payloadDataType = fixPartBuffer.getInt();
        
        // In Go, PostHeaderLengths is a []uint8 with one entry per event type
        // First determine how many event types we have
        int numEventTypes = EventType.values().length;
        
        byte[] postHeaderLengths = new byte[numEventTypes];
        bytesRead = reader.read(postHeaderLengths);
        
        if (bytesRead != numEventTypes) {
            throw new IOException(String.format(
                "Failed to read post header lengths: expected %d bytes, got %d bytes",
                numEventTypes, bytesRead));
        }
        
        // Read extra data length and extra data
        ByteBuffer extraLengthBuffer = ByteBuffer.allocate(4);
        extraLengthBuffer.order(ByteOrder.LITTLE_ENDIAN);
        bytesRead = reader.read(extraLengthBuffer.array());
        
        if (bytesRead != 4) {
            throw new IOException(String.format(
                "Failed to read extra length: expected 4 bytes, got %d bytes",
                bytesRead));
        }
        
        int extraLength = extraLengthBuffer.getInt(0);
        
        // Sanity check for extra length
        if (extraLength < 0 || extraLength > 1024 * 1024) { // Max 1MB
            throw new IOException(String.format(
                "Invalid extra length: %d (must be >= 0 and <= 1MB)",
                extraLength));
        }
        
        byte[] extraData = new byte[extraLength];
        if (extraLength > 0) {
            bytesRead = reader.read(extraData);
        if (bytesRead != extraLength) {
            throw new IOException(String.format(
                    "Failed to read extra data: expected %d bytes, got %d bytes at position %d (file length: %d)",
                    extraLength, bytesRead, reader.position(), reader.length()));
            }
        }
        
        // Create and return the descriptor event data
        return new DescriptorEventData(
            collectionID,
            partitionID,
            segmentID,
            fieldID,
            startTimestamp,
            endTimestamp,
            payloadDataType,
            postHeaderLengths,
            extraData
        );
    }
    
    /**
     * Reads all events from the file.
     *
     * @return List of delta data objects
     * @throws IOException If an I/O error occurs
     */
    public List<DeltaData> readAll() throws IOException {
        // Clear existing data
        deltaDataList.clear();
        
        logger.info("Starting to read events from delta log file");
        int eventCount = 0;
        
        // Try to read events until end of file
        DeltaData deltaData;
        while ((deltaData = readNextEvent()) != null) {
            eventCount++;
            logger.info("Read event #{}", eventCount);
            
            // Add all non-null delta data objects
            deltaDataList.add(deltaData);
        }
        
        logger.info("Finished reading events. Total events: {}", eventCount);
        logger.info("Total delta data entries: {}", deltaDataList.size());
        
        // Print detailed information about each delta data entry
        for (int i = 0; i < deltaDataList.size(); i++) {
            DeltaData data = deltaDataList.get(i);
            // Use reflection to access information about the Delta data since methods may not be exposed
            try {
                Field pkCountField = DeltaData.class.getDeclaredField("deleteRowCount");
                pkCountField.setAccessible(true);
                long pkCount = pkCountField.getLong(data);
                
                logger.debug("Delta data #{}: data type={}, pk count={}", 
                           (i+1), data.getDataType(), pkCount);
            } catch (Exception e) {
                logger.error("Error getting delta data details: {}", e.getMessage());
            }
        }
        
        return deltaDataList;
    }
    
    /**
     * Reads the next event from the file.
     * 
     * This method reads the event header to determine the event type,
     * then delegates to specialized methods for different event types.
     * The event types include:
     * - INSERT_EVENT: Adding new data
     * - DELETE_EVENT: Marking data as deleted
     * - INDEX_FILE_EVENT: Changes to indices
     *
     * @return The next DeltaData, or null if EOF or if the event type is unsupported
     * @throws IOException If an I/O error occurs during reading
     */
    private DeltaData readNextEvent() throws IOException {
        // Read event header
        byte[] headerBytes = new byte[EventHeader.EVENT_SIZE];
        int bytesRead = reader.read(headerBytes);
        
        // Check for EOF
        if (bytesRead <= 0) {
            return null; // EOF
        }
        
        // Check for incomplete header
        if (bytesRead != EventHeader.EVENT_SIZE) {
            throw new IOException(String.format(
                "Failed to read event header: expected %d bytes, got %d bytes",
                EventHeader.EVENT_SIZE, bytesRead));
        }

        // Parse header with correct byte order (little-endian)
        ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // Set to little-endian to match Go implementation
        
        // Read header fields manually to ensure correct byte order
        long timestamp = buffer.getLong();
        byte eventTypeByte = buffer.get();
        int eventLength = buffer.getInt();
        int nextPosition = buffer.getInt();
        
        // Get event type and validate it
        int eventTypeCode = eventTypeByte & 0xFF;
        EventType eventType;
        try {
            eventType = EventType.fromCode(eventTypeCode);
        } catch (IllegalArgumentException e) {
            // Skip this event and return null
            if (eventLength > EventHeader.EVENT_SIZE) {
                reader.skip(eventLength - EventHeader.EVENT_SIZE);
            }
            return null;
        }
        
        // Sanity check for event length
        if (eventLength < EventHeader.EVENT_SIZE || eventLength > 100 * 1024 * 1024) { // Max 100MB
            throw new IOException(String.format(
                "Invalid event length: %d (must be >= %d and <= 100MB)", 
                eventLength, EventHeader.EVENT_SIZE));
        }

        // Process different event types
        try {
            switch (eventType) {
                case INSERT_EVENT:
                case DELETE_EVENT:
                    return readDataEvent(eventLength);
                case INDEX_FILE_EVENT:
                    return readIndexFileEvent(eventLength);
                default:
                    // Skip other event types
                    reader.skip(eventLength - EventHeader.EVENT_SIZE);
                    return null;
            }
        } catch (IOException e) {
            throw new IOException("Error reading event data for " + eventType + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Reads a data event (INSERT or DELETE event).
     * 
     * Data events contain:
     * 1. Timestamps (start and end) - 16 bytes
     * 2. Payload data - The actual data in Parquet format
     *
     * @param eventLength The total length of the event
     * @return The delta data containing the parsed event information
     * @throws IOException If an I/O error occurs
     */
    private DeltaData readDataEvent(int eventLength) throws IOException {
        // Read timestamps (startTimestamp(8) + endTimestamp(8) = 16 bytes)
        byte[] timestampBytes = new byte[16];
        int bytesRead = reader.read(timestampBytes);
        if (bytesRead != 16) {
            throw new IOException(String.format(
                "Failed to read event timestamps: expected 16 bytes, got %d bytes",
                bytesRead));
        }
        
        // Parse timestamps with correct byte order
        ByteBuffer buffer = ByteBuffer.wrap(timestampBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long startTimestamp = buffer.getLong();
        long endTimestamp = buffer.getLong();
        
        logger.debug("Event timestamps: start={}, end={}", startTimestamp, endTimestamp);
        
        // Calculate payload size (eventLength - headerSize - timestampSize)
        int payloadSize = eventLength - EventHeader.EVENT_SIZE - 16;
        if (payloadSize < 0) {
            throw new IOException(String.format(
                "Invalid payload size: %d (event length: %d, header size: %d, timestamp size: 16)",
                payloadSize, eventLength, EventHeader.EVENT_SIZE));
        }
        
        logger.debug("Reading payload data of size: {} bytes", payloadSize);
        
        // Create delta data with the timestamps
        DeltaData deltaData = new DeltaData(startTimestamp, endTimestamp);
        
        // If there's no payload data, just return the empty delta data
        if (payloadSize == 0) {
            logger.info("No payload data to parse");
            return deltaData;
        }
        
        // Read payload data (in Parquet format)
        byte[] payloadData = new byte[payloadSize];
        bytesRead = reader.read(payloadData);
        if (bytesRead != payloadSize) {
            throw new IOException(String.format(
                "Failed to read payload data: expected %d bytes, got %d bytes",
                payloadSize, bytesRead));
        }
        
        // Parse payload data
        parseEventData(payloadData, deltaData);
        
        logger.debug("Completed reading data event");
        return deltaData;
    }
    
    /**
     * Reads an index file event.
     *
     * @param eventLength The total length of the event
     * @return The delta data
     * @throws IOException If an I/O error occurs
     */
    private DeltaData readIndexFileEvent(int eventLength) throws IOException {
        // Read timestamps (startTimestamp(8) + endTimestamp(8) = 16 bytes)
        byte[] timestampBytes = new byte[16];
        int bytesRead = reader.read(timestampBytes);
        if (bytesRead != 16) {
            throw new IOException(String.format(
                "Failed to read index event timestamps: expected 16 bytes, got %d bytes",
                bytesRead));
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(timestampBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        long startTimestamp = buffer.getLong();
        long endTimestamp = buffer.getLong();
        
        // Calculate payload size (event length - header size - timestamp size)
        int payloadSize = eventLength - EventHeader.EVENT_SIZE - 16;
        if (payloadSize < 0) {
            throw new IOException(String.format(
                "Invalid index payload size: %d (event length: %d, header size: %d, timestamp size: 16)",
                payloadSize, eventLength, EventHeader.EVENT_SIZE));
        }
        
        // Skip payload data for now
        if (payloadSize > 0) {
            reader.skip(payloadSize);
        }
        
        // Return null since we don't process index file events yet
        return null;
    }
    
    /**
     * Parses event data from Parquet format.
     * 
     * The event data is in Parquet format and contains primary keys and timestamps.
     * This method extracts the primary keys and associated timestamps and adds them
     * to the provided DeltaData object.
     *
     * @param data The event data in Parquet format
     * @param deltaData The delta data to update with parsed information
     * @throws IOException If an I/O error occurs during parsing
     */
    private void parseEventData(byte[] data, DeltaData deltaData) throws IOException {
        if (data.length == 0) {
            logger.info("Empty payload data, skipping");
            return;
        }
        
        logger.info("Parsing payload data: {} bytes", data.length);
        
        try {
            // Try to parse using ParquetPayloadReader
            ParquetPayloadReader reader = new ParquetPayloadReader(dataType, data);
            
            // First try the approach with column indices
            boolean success = parseWithColumnIndices(reader, deltaData);
            
            // If that fails, try to parse assuming a single column with primary keys
            if (!success) {
                success = parseWithSingleColumn(reader, deltaData);
            }
            
            // If still no success, throw an exception
            if (!success) {
                throw new IOException("Failed to parse data using all available strategies");
            }
        } catch (Exception e) {
            logger.error("Error parsing with ParquetPayloadReader: {}", e.getMessage());
            throw new IOException("Error parsing event data: " + e.getMessage(), e);
        }
    }
    
    /**
     * Parse data using column indices approach
     */
    private boolean parseWithColumnIndices(ParquetPayloadReader reader, DeltaData deltaData) throws IOException {
        // Column indices based on the format specification
        // Primary keys are in column 0, timestamps are in column 1
        final int PK_COLUMN_INDEX = 0;
        final int TS_COLUMN_INDEX = 1;
        
        boolean dataAdded = false;
        
        // Based on the data type, extract primary keys
        if (dataType == DataType.STRING) {
            List<String> primaryKeys = reader.getStringFromPayload(PK_COLUMN_INDEX);
            
            // If we have primary keys
            if (!primaryKeys.isEmpty()) {
                logger.info("Found {} string primary keys", primaryKeys.size());
                
                // Try to get timestamps, but if not available, use a default timestamp
                List<Long> timestamps = reader.getInt64FromPayload(TS_COLUMN_INDEX);
                long defaultTimestamp = System.currentTimeMillis();
                
                logger.info("Found {} timestamps", timestamps.size());
                
                // Add each primary key with its timestamp
                for (int i = 0; i < primaryKeys.size(); i++) {
                    StringPrimaryKey pk = new StringPrimaryKey(primaryKeys.get(i));
                    long ts = (i < timestamps.size()) ? timestamps.get(i) : defaultTimestamp;
                    deltaData.append(pk, ts);
                    dataAdded = true;
                }
            } else {
                logger.debug("No string primary keys found using column indices approach");
            }
        } else if (dataType == DataType.INT64) {
            List<Long> primaryKeys = reader.getInt64FromPayload(PK_COLUMN_INDEX);
            
            // If we have primary keys
            if (!primaryKeys.isEmpty()) {
                logger.info("Found {} int64 primary keys", primaryKeys.size());
                
                // Try to get timestamps, but if not available, use a default timestamp
                List<Long> timestamps = reader.getInt64FromPayload(TS_COLUMN_INDEX);
                long defaultTimestamp = System.currentTimeMillis();
                
                logger.info("Found {} timestamps", timestamps.size());
                
                // Add each primary key with its timestamp
                for (int i = 0; i < primaryKeys.size(); i++) {
                    Int64PrimaryKey pk = new Int64PrimaryKey(primaryKeys.get(i));
                    long ts = (i < timestamps.size()) ? timestamps.get(i) : defaultTimestamp;
                    deltaData.append(pk, ts);
                    dataAdded = true;
                }
            } else {
                logger.debug("No int64 primary keys found using column indices approach");
            }
        } else {
            logger.warn("Unsupported data type for Parquet parsing: {}", dataType);
        }
        
        return dataAdded;
    }
    
    /**
     * Parse data assuming a single column with primary keys
     */
    private boolean parseWithSingleColumn(ParquetPayloadReader reader, DeltaData deltaData) throws IOException {
        boolean dataAdded = false;
        
        // Use only column 0, assuming all primary keys
        final int COLUMN_INDEX = 0;
        long defaultTimestamp = System.currentTimeMillis();
        
        if (dataType == DataType.STRING) {
            List<String> values = reader.getStringFromPayload(COLUMN_INDEX);
            if (!values.isEmpty()) {
                logger.info("Found {} string values in column 0", values.size());
                
                for (String value : values) {
                    StringPrimaryKey pk = new StringPrimaryKey(value);
                    deltaData.append(pk, defaultTimestamp);
                    dataAdded = true;
                }
            } else {
                logger.debug("No string values found in column 0");
            }
        } else if (dataType == DataType.INT64) {
            List<Long> values = reader.getInt64FromPayload(COLUMN_INDEX);
            if (!values.isEmpty()) {
                logger.info("Found {} int64 values in column 0", values.size());
                
                for (Long value : values) {
                    Int64PrimaryKey pk = new Int64PrimaryKey(value);
                    deltaData.append(pk, defaultTimestamp);
                    dataAdded = true;
                }
            } else {
                logger.debug("No int64 values found in column 0");
            }
        }
        
        return dataAdded;
    }
    
    /**
     * Gets an iterator over all delta data.
     *
     * @return An iterator over DeltaData objects
     */
    @Override
    public Iterator<DeltaData> iterator() {
        return new Iterator<DeltaData>() {
            @Override
            public boolean hasNext() {
                try {
                    return reader.length() - reader.position() > 0;
                } catch (IOException e) {
                    return false;
                }
            }

            @Override
            public DeltaData next() {
                try {
                    return readNextEvent();
                } catch (IOException e) {
                    throw new RuntimeException("Error reading next event", e);
                }
            }
        };
    }
    
    /**
     * Gets the data type of this delta log.
     *
     * @return The data type
     */
    public DataType getDataType() {
        return dataType;
    }
    
    /**
     * Gets the number of delta data entries.
     *
     * @return The number of entries
     */
    public int size() {
        return deltaDataList.size();
    }
    
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
} 