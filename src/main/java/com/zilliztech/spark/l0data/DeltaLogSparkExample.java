package com.zilliztech.spark.l0data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Example class demonstrating how to use DeltaLogReader to read Milvus delta log files
 * and convert the data to a Spark DataFrame for analysis and processing.
 * 
 * This implementation handles both STRING and INT64 primary key types and structures the
 * data in a format suitable for analytical processing using Spark SQL.
 * 
 * Delta log files contain a series of events (insertions, deletions) with primary keys
 * and timestamps. This class extracts those events and transforms them into a format
 * that can be easily analyzed with Spark.
 */
public class DeltaLogSparkExample {
    private static final Logger logger = LoggerFactory.getLogger(DeltaLogSparkExample.class);

    /**
     * Main method demonstrating usage of the delta log to DataFrame conversion.
     * This method:
     * 1. Initializes a Spark session
     * 2. Reads one or more delta log files and converts them to a DataFrame
     * 3. Performs basic DataFrame operations to demonstrate functionality
     * 4. Optionally saves the DataFrame as a Parquet file
     * 
     * @param args Command line arguments:
     *             - First argument and subsequent arguments: paths to delta log files
     *             - Last argument (optional): output path if prefixed with "--output="
     */
    public static void main(String[] args) {
        // Process command line arguments
        List<String> deltaLogPaths = new ArrayList<>();
        String outputPath = null;
        
        // Parse arguments
        for (String arg : args) {
            if (arg.startsWith("--output=")) {
                outputPath = arg.substring("--output=".length());
            } else {
                deltaLogPaths.add(arg);
            }
        }

        String demoPath = "src/main/resources/l0_delta_log_for_testing";

        // If no paths provided, use default path
        if (deltaLogPaths.isEmpty()) {
            deltaLogPaths.add(demoPath);
        }
        
        // Initialize Spark session with local mode
        SparkSession spark = DeltaLogUtils.createLocalSparkSession("DeltaLog to Spark DataFrame");
        
        try {
            logger.info("Starting DeltaLogSparkExample with files:");
            deltaLogPaths.forEach(path -> logger.info("  - {}", path));
            
            // Read delta log files and convert to DataFrame
            long startTime = System.currentTimeMillis();
            Dataset<Row> deltaDF = DeltaLogUtils.deltaLogsToDataFrame(deltaLogPaths, spark);
            long endTime = System.currentTimeMillis();
            
            logger.info("Delta log reading completed in {} ms", (endTime - startTime));
            
            // Show the schema and some sample data
            logger.info("DataFrame Schema:");
            deltaDF.printSchema();
            
            logger.info("Sample Data (first 10 rows):");
            deltaDF.show(10, false); // false to not truncate string columns
            
            // Demonstrate some DataFrame operations
            logger.info("Basic DataFrame statistics:");
            logger.info("Total rows: {}", deltaDF.count());
            
            logger.info("Unique primary key count:");
            long uniqueCount = deltaDF.select("pk").distinct().count();
            logger.info("{}", uniqueCount);
            
            // Show value distribution for extracted fields
            logger.info("Distribution of pk values:");
            deltaDF.groupBy("pk").count().orderBy("pk").show(20);
            
            logger.info("Min and max timestamps:");
            deltaDF.agg(min("ts").as("min_ts"), max("ts").as("max_ts")).show();
            
            // Distribution of timestamps
            logger.info("Timestamp distribution (summary statistics):");
            deltaDF.select(col("ts"))
                   .summary("min", "25%", "50%", "75%", "max")
                   .show();
                   
            // If multiple files were processed, show statistics per file
            if (deltaLogPaths.size() > 1) {
                logger.info("Statistics for multiple delta log files:");
                
                // Get total row count across all files
                logger.info("Total rows across all files: {}", deltaDF.count());
                
                // Show primary key distributions 
                logger.info("Top 20 primary keys by frequency:");
                deltaDF.groupBy("pk")
                      .count()
                      .orderBy(desc("count"))
                      .show(20);
            }
            
            // Save the DataFrame as Parquet file if output path is specified
            if (outputPath != null) {
                logger.info("Saving DataFrame to: {}", outputPath);
                deltaDF.write().mode(SaveMode.Overwrite).parquet(outputPath);
                logger.info("DataFrame saved successfully.");
            }
            
        } catch (Exception e) {
            logger.error("Error processing delta log files: {}", e.getMessage(), e);
        } finally {
            // Stop Spark session when done to free resources
            spark.stop();
            logger.info("Spark session stopped. Example completed.");
        }
    }
    
    /**
     * Process multiple delta log files and combine them into a single DataFrame.
     * This is a convenience method useful for programmatic access.
     * 
     * @param paths An array of paths to delta log files
     * @return A Spark DataFrame containing all data from the specified files
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> processDeltaLogFiles(String... paths) throws IOException {
        // Validate input
        if (paths == null || paths.length == 0) {
            throw new IllegalArgumentException("At least one delta log file path must be provided");
        }
        
        // Initialize Spark
        SparkSession spark = DeltaLogUtils.createLocalSparkSession("Delta Log Processor");
        
        try {
            // Process all files
            return DeltaLogUtils.deltaLogsToDataFrame(Arrays.asList(paths), spark);
        } catch (IOException e) {
            logger.error("Error processing delta log files: {}", e.getMessage());
            throw e;
        }
    }
} 