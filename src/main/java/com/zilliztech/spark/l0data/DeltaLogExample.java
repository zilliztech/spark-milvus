package com.zilliztech.spark.l0data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example class demonstrating how to use DeltaLogReader to read l0_delta files.
 */
public class DeltaLogExample {
    private static final Logger logger = LoggerFactory.getLogger(DeltaLogExample.class);

    /**
     * Reads a l0_delta file and processes its contents.
     *
     * @param filePath The path to the l0_delta file
     * @return A list of processed delta data
     * @throws IOException If an I/O error occurs
     */
    public static List<DeltaData> readL0DeltaFile(String filePath) throws IOException {
        List<DeltaData> results = new ArrayList<>();
        
        try (DeltaLogReader reader = new DeltaLogReader(filePath)) {
            // Print descriptor event details
            DescriptorEvent descriptorEvent = reader.getDescriptorEvent();
            if (descriptorEvent != null) {
                DescriptorEventHeader header = descriptorEvent.getHeader();
                DescriptorEventData data = descriptorEvent.getData();
                logger.info("Descriptor Event:");
                logger.info("  Collection ID: {}", data.getCollectionID());
                logger.info("  Partition ID: {}", data.getPartitionID());
                logger.info("  Segment ID: {}", data.getSegmentID());
                logger.info("  Field ID: {}", data.getFieldID());
                logger.info("  Start Timestamp: {}", data.getStartTimestamp());
                logger.info("  End Timestamp: {}", data.getEndTimestamp());
                logger.info("  Payload Data Type: {}", DataType.fromCode(data.getPayloadDataType()));
                logger.info("  Post Header Lengths: {}", Arrays.toString(data.getPostHeaderLengths()));
                logger.info("  Extras: {}", data.getExtras());
            } else {
                logger.warn("No Descriptor Event found.");
            }
            
            // Read all events
            results = reader.readAll();
        }
        
        return results;
    }

    
    /**
     * Main method demonstrating usage.
     */
    public static void main(String[] args) {
        try {
            String path = args.length > 0 ? args[0] : "src/main/resources/l0_delta_log_for_testing";
            List<DeltaData> results;

            logger.info("Reading file: {}", path);
            results = readL0DeltaFile(path);
            
            logger.info("-----------------------------------------------");
            logger.info("Delta Log Summary:");
            logger.info("  Total delta data entries: {}", results.size());
            
            // Print summary
            if (!results.isEmpty()) {
                for (int i = 0; i < results.size(); i++) {
                    DeltaData deltaData = results.get(i);
                    logger.info("\nDelta Data Entry #{}", (i+1));
                    logger.info("  Data type: {}", deltaData.getDataType());
                    logger.info("  Delete row count: {}", deltaData.getDeleteRowCount());
                    
                    // Get timestamps (first two values are start/end timestamps)
                    List<Long> timestamps = deltaData.getTimestamps();
                    if (timestamps.size() >= 2) {
                        logger.info("  Start timestamp: {}", timestamps.get(0));
                        logger.info("  End timestamp: {}", timestamps.get(1));
                    }
                    
                    // Count primary keys by timestamp - helpful for debugging
                    Map<Long, Integer> timestampCounts = new HashMap<>();
                    
                    logger.info("\n  Primary Keys:");
                    // Use the range method to go through each PK/timestamp pair
                    final int[] count = {0};
                    deltaData.range((pk, timestamp) -> {
                        count[0]++;
                        // Only print first 10 and last 10 entries to avoid flooding console
                        if (count[0] <= 10 || count[0] > deltaData.getDeleteRowCount() - 10) {
                            String displayValue = pk.getValue().toString();
                            // For string PKs, trim if too long
                            if (displayValue.length() > 50) {
                                displayValue = displayValue.substring(0, 47) + "...";
                            }
                            logger.info("    PK: {} | Timestamp: {}", displayValue, timestamp);
                        } else if (count[0] == 11) {
                            logger.info("    ... (skipping {} entries) ...", 
                                      (deltaData.getDeleteRowCount() - 20));
                        }
                        
                        // Count timestamps
                        timestampCounts.put(timestamp, timestampCounts.getOrDefault(timestamp, 0) + 1);
                        return true; // Continue iteration
                    });
                    
                    // Print timestamp statistics
                    logger.info("\n  Timestamp Distribution:");
                    timestampCounts.forEach((ts, cnt) -> 
                        logger.info("    Timestamp: {} | Count: {}", ts, cnt));
                }
            } else {
                logger.warn("\nNo delta data entries found!");
                logger.info("Please verify the delta log file exists and has valid content.");
            }
            
        } catch (IOException e) {
            logger.error("\n[ERROR] Failed to process delta log file:", e);
        } catch (Exception e) {
            logger.error("\n[FATAL ERROR] Unexpected error:", e);
        }
    }
} 