package com.zilliztech.spark.l0data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.spark.sql.functions.*;

/**
 * Utility class for processing Milvus delta log files with Spark.
 * Provides methods to convert multiple delta log files into a single DataFrame.
 */
public class DeltaLogUtils {
    private static final Logger logger = LoggerFactory.getLogger(DeltaLogUtils.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final StructType DEFAULT_SCHEMA = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField("pk", DataTypes.LongType, false),
        DataTypes.createStructField("ts", DataTypes.LongType, false)
    });

    /**
     * Parses a JSON primary key string to extract its components.
     * 
     * @param pkString The JSON string representing the primary key
     * @return An array of Objects containing [pkId, pkTs]
     */
    private static Object[] parseJsonPk(String pkString) {
        try {
            if (pkString != null && pkString.startsWith("{") && pkString.endsWith("}")) {
                JsonNode jsonNode = objectMapper.readTree(pkString);
                
                // Extract fields
                Long pkId = null;
                if (jsonNode.has("pk")) {
                    pkId = jsonNode.get("pk").asLong();
                }
                
                Long pkTs = null;
                if (jsonNode.has("ts")) {
                    pkTs = jsonNode.get("ts").asLong();
                }
                
                return new Object[] { pkId, pkTs };
            }
        } catch (Exception e) {
            logger.warn("Error parsing JSON primary key: {}", e.getMessage());
        }
        
        // If not JSON or parsing failed, try to convert to Long directly
        try {
            if (pkString != null) {
                return new Object[] { Long.parseLong(pkString), null };
            }
        } catch (NumberFormatException e) {
            // Not a number, return null
        }
        
        // If all parsing fails, return nulls
        return new Object[] { null, null };
    }

    public static Dataset<Row> createDataFrameV2(List<String> filePaths, SparkSession spark) throws IOException {
        return createDataFrameV2(filePaths, spark, DataTypes.LongType);
    }

    /**
     * Creates a DataFrame from delta log files.
     * 
     * @param filePaths List of paths to delta log files
     * @param spark The SparkSession to use
     * @return A DataFrame with pk and ts columns
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> createDataFrameV2(List<String> filePaths, SparkSession spark,
                                                    org.apache.spark.sql.types.DataType pkDataType) throws IOException {
        if (filePaths == null || filePaths.isEmpty()) {
            throw new IllegalArgumentException("File paths list cannot be empty");
        }

        logger.info("Processing {} delta log files in createDataFrame with pkType {}", filePaths.size(), pkDataType);

        // Define custom schema with provided primary key type
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("pk", pkDataType, false),
                DataTypes.createStructField("ts", DataTypes.LongType, false)
        });

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<String> filePathsRDD = jsc.parallelize(filePaths);
        
        // Process each file
        JavaRDD<Row> rowsRDD = filePathsRDD.flatMap(filePath -> {
            List<Row> rows = new ArrayList<>();
            
            try (DeltaLogReader reader = new DeltaLogReader(filePath)) {
                List<DeltaData> deltaDataList = reader.readAll();
                
                for (DeltaData deltaData : deltaDataList) {
                    final AtomicInteger count = new AtomicInteger(0);
                    
                    deltaData.range((pk, timestamp) -> {
                        String pkValue;
                        if (pk instanceof Int64PrimaryKey) {
                            pkValue = String.valueOf(((Int64PrimaryKey) pk).getValue());
                        } else if (pk instanceof StringPrimaryKey) {
                            pkValue = ((StringPrimaryKey) pk).getValue();
                        } else {
                            pkValue = pk.toString();
                        }

                        // Parse JSON primary key to extract its components
                        Object[] parsedPk = parseJsonPk(pkValue);
                        Object pk_id;
                        if (pkDataType.equals(DataTypes.StringType)) {
                            pk_id = parsedPk[0] != null ? parsedPk[0].toString() : null;
                        } else {
                            pk_id = parsedPk[0];
                        }
                        Long ts = (Long) parsedPk[1];
                        
                        rows.add(RowFactory.create(
                            pk_id,      // pk
                            ts          // ts (from JSON, or event timestamp if not available)
                        ));
                        count.incrementAndGet();
                        return true;
                    });
                    
                    logger.info("Processed {} rows from file: {}", count.get(), filePath);
                }
            } catch (IOException e) {
                logger.error("Error processing file {}: {}", filePath, e.getMessage());
                throw e;
            }
            
            return rows.iterator();
        });
        
        // Create DataFrame using the default schema
        return spark.createDataFrame(rowsRDD, DEFAULT_SCHEMA);
    }

    /**
     * Converts multiple delta log files into a single DataFrame.
     * The resulting DataFrame has pk and ts columns.
     * 
     * @param filePaths List of paths to delta log files
     * @param spark The SparkSession to use
     * @return A DataFrame containing data from all specified delta log files
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> deltaLogsToDataFrame(List<String> filePaths, SparkSession spark) throws IOException {
        if (filePaths == null || filePaths.isEmpty()) {
            throw new IllegalArgumentException("File paths list cannot be empty");
        }
        
        // Process files in parallel using Spark
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        
        // Convert the list of file paths to an RDD
        JavaRDD<String> filePathsRDD = jsc.parallelize(filePaths);
        
        // Process each file and collect all rows
        JavaRDD<Row> rowsRDD = filePathsRDD.flatMap(filePath -> {
            List<Row> rows = new ArrayList<>();
            
            try (DeltaLogReader reader = new DeltaLogReader(filePath)) {
                // Read all delta data from this file
                List<DeltaData> deltaDataList = reader.readAll();
                
                // Process each delta data entry
                for (DeltaData deltaData : deltaDataList) {
                    // Extract primary key and timestamp pairs
                    final AtomicInteger count = new AtomicInteger(0);
                    
                    deltaData.range((pk, timestamp) -> {
                        // Convert primary key to string representation
                        String pkValue;
                        if (pk instanceof Int64PrimaryKey) {
                            pkValue = String.valueOf(pk.getValue());
                        } else if (pk instanceof StringPrimaryKey) {
                            pkValue = ((StringPrimaryKey) pk).getValue();
                        } else {
                            pkValue = pk.toString();
                        }
                        
                        // Parse JSON primary key to extract its components
                        Object[] parsedPk = parseJsonPk(pkValue);
                        Long pk_id = (Long) parsedPk[0];
                        Long ts = (Long) parsedPk[1];
                        
                        // Use the JSON timestamp if available, otherwise use the event timestamp
                        if (ts == null) {
                            ts = timestamp;
                        }
                        
                        rows.add(RowFactory.create(
                            pk_id,      // pk
                            ts          // ts (from JSON, or event timestamp if not available)
                        ));
                        count.incrementAndGet();
                        return true; // Continue iteration
                    });
                    
                    logger.info("Processed {} rows from file: {}", count.get(), new File(filePath).getName());
                }
            } catch (IOException e) {
                logger.error("Error processing file {}: {}", filePath, e.getMessage());
                throw e;
            }
            
            return rows.iterator();
        });
        
        // Create DataFrame from the RDD using the default schema
        Dataset<Row> deltaDF = spark.createDataFrame(rowsRDD, DEFAULT_SCHEMA);
        
        return deltaDF;
    }
    
    /**
     * Converts a delta log file into a DataFrame.
     * 
     * @param filePath Path to a delta log file
     * @param spark The SparkSession to use
     * @return A DataFrame containing data from the delta log file
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> deltaLogToDataFrame(String filePath, SparkSession spark) throws IOException {
        return deltaLogsToDataFrame(Lists.newArrayList(filePath), spark);
    }
    
    /**
     * Converts multiple delta log files matching a glob pattern into a single DataFrame.
     * 
     * @param fileGlob A glob pattern for delta log files (e.g., "/path/to/binlogs/delta_log/l*_data")
     * @param spark The SparkSession to use
     * @return A DataFrame containing data from all matching delta log files
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> deltaLogsFromPattern(String fileGlob, SparkSession spark) throws IOException {
        // Expand glob pattern to find matching files
        List<String> matchingFiles = expandGlobPattern(fileGlob);
        if (matchingFiles.isEmpty()) {
            throw new IOException("No files found matching pattern: " + fileGlob);
        }
        
        logger.info("Found {} files matching pattern: {}", matchingFiles.size(), fileGlob);
        
        return deltaLogsToDataFrame(matchingFiles, spark);
    }
    
    /**
     * Analyzes delta log files for primary key statistics.
     * 
     * @param filePaths List of paths to delta log files
     * @param spark The SparkSession to use
     * @return A DataFrame containing analysis of primary keys
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> analyzeKeyDistribution(List<String> filePaths, SparkSession spark) throws IOException {
        // Get the combined DataFrame
        Dataset<Row> df = createDataFrame(filePaths, spark);
        
        // Group by primary key and analyze timestamps
        Dataset<Row> keyAnalysis = df.groupBy("pk")
            .agg(
                count("*").as("occurrence_count"),
                min("ts").as("first_seen_ts"),
                max("ts").as("last_seen_ts")
            )
            .withColumn("time_difference_ms", col("last_seen_ts").minus(col("first_seen_ts")));
        
        logger.info("Analyzed {} unique primary keys", keyAnalysis.count());
        
        return keyAnalysis.orderBy(desc("occurrence_count"));
    }
    
    /**
     * Compares two delta log files to identify common and different primary keys.
     * 
     * @param file1 Path to the first delta log file
     * @param file2 Path to the second delta log file
     * @param spark The SparkSession to use
     * @return A DataFrame with common keys and their statistics
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> compareKeys(String file1, String file2, SparkSession spark) throws IOException {
        // Get DataFrames for both files
        Dataset<Row> df1 = deltaLogToDataFrame(file1, spark);
        Dataset<Row> df2 = deltaLogToDataFrame(file2, spark);
        
        // Get unique keys from each file
        Dataset<Row> keys1 = df1.select("pk").distinct();
        Dataset<Row> keys2 = df2.select("pk").distinct();
        
        // Extract file names for status messages
        String file1Name = new File(file1).getName();
        String file2Name = new File(file2).getName();
        
        // Find common keys
        Dataset<Row> commonKeys = keys1.intersect(keys2);
        
        // Find keys in file1 but not in file2
        Dataset<Row> inFile1Only = keys1.except(keys2);
        
        // Find keys in file2 but not in file1
        Dataset<Row> inFile2Only = keys2.except(keys1);
        
        long commonCount = commonKeys.count();
        long in1Only = inFile1Only.count();
        long in2Only = inFile2Only.count();
        long count1 = keys1.count();
        long count2 = keys2.count();
        
        logger.info("File {} has {} unique keys", file1Name, count1);
        logger.info("File {} has {} unique keys", file2Name, count2);
        logger.info("Common keys: {}", commonCount);
        logger.info("Keys only in {}: {}", file1Name, in1Only);
        logger.info("Keys only in {}: {}", file2Name, in2Only);
        
        // Join with original dataframes to get timestamp info for common keys
        Dataset<Row> df1WithLabel = df1.withColumn("source", lit(file1Name));
        Dataset<Row> df2WithLabel = df2.withColumn("source", lit(file2Name));
        Dataset<Row> combinedDF = df1WithLabel.union(df2WithLabel);
        
        // Get statistics for common keys
        return combinedDF.join(commonKeys, "pk")
            .groupBy("pk")
            .agg(
                collect_list("ts").as("timestamps"),
                collect_list("source").as("sources"),
                min("ts").as("min_ts"),
                max("ts").as("max_ts")
            )
            .orderBy("pk");
    }
    
    /**
     * Expands a glob pattern to find matching files.
     * 
     * @param fileGlob A glob pattern for delta log files
     * @return A list of file paths matching the pattern
     * @throws IOException If an I/O error occurs
     */
    public static List<String> expandGlobPattern(String fileGlob) throws IOException {
        // Simple implementation that supports asterisk wildcards
        // For more complex glob patterns, consider using Java NIO or libraries like Apache Commons IO
        
        File file = new File(fileGlob);
        
        // If no wildcards, just check if file exists
        if (!fileGlob.contains("*") && !fileGlob.contains("?")) {
            if (file.exists()) {
                return Lists.newArrayList(fileGlob);
            }
            return Lists.newArrayList();
        }
        
        // Handle glob pattern with wildcard
        String dirPath = file.getParent();
        if (dirPath == null) {
            dirPath = ".";
        }
        
        String filePattern = file.getName();
        
        // Convert glob pattern to regex pattern
        String regexPattern = filePattern
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".");
        
        List<String> matchingFiles = new ArrayList<>();
        File dir = new File(dirPath);

        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.getName().matches(regexPattern) && f.isFile()) {
                        matchingFiles.add(f.getAbsolutePath());
                    }
                }
            }
        }
        return matchingFiles;
    }
    
    /**
     * Creates a new SparkSession for local processing.
     * 
     * @param appName The application name
     * @return A configured SparkSession
     */
    public static SparkSession createLocalSparkSession(String appName) {
        return SparkSession.builder()
            .appName(appName)
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate();
    }

    /**
     * Creates a DataFrame directly from in-memory data structures without using RDD.
     * This is optimized for small files where the overhead of distributed processing is not needed.
     * 
     * @param filePaths List of paths to delta log files
     * @param spark The SparkSession to use
     * @param pkDataType The data type to use for the primary key
     * @return A DataFrame with pk and ts columns
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> createDataFrame(List<String> filePaths, SparkSession spark, 
                                             org.apache.spark.sql.types.DataType pkDataType) throws IOException {
        if (filePaths == null || filePaths.isEmpty()) {
            throw new IllegalArgumentException("File paths list cannot be empty");
        }
        
        logger.info("Processing {} delta log files in createDataFrame with pkType {}", filePaths.size(), pkDataType);
        
        // Define custom schema with provided primary key type
        StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("pk", pkDataType, false),
            DataTypes.createStructField("ts", DataTypes.LongType, false)
        });
        
        // Process all files in memory first
        List<Row> allRows = new ArrayList<>();
        int totalProcessed = 0;
        
        for (String filePath : filePaths) {
            try (DeltaLogReader reader = new DeltaLogReader(filePath)) {
                List<DeltaData> deltaDataList = reader.readAll();
                final AtomicInteger fileRowCount = new AtomicInteger(0);
                
                for (DeltaData deltaData : deltaDataList) {
                    deltaData.range((pk, timestamp) -> {
                        // Convert primary key to string representation
                        String pkValue;
                        if (pk instanceof Int64PrimaryKey) {
                            pkValue = String.valueOf(pk.getValue());
                        } else if (pk instanceof StringPrimaryKey) {
                            pkValue = ((StringPrimaryKey) pk).getValue();
                        } else {
                            pkValue = pk.toString();
                        }

                        // Parse JSON primary key to extract its components
                        Object[] parsedPk = parseJsonPk(pkValue);
                        Object pk_id;
                        if (pkDataType.equals(DataTypes.StringType)) {
                            pk_id = parsedPk[0] != null ? parsedPk[0].toString() : null;
                        } else {
                            pk_id = parsedPk[0];
                        }
                        Long ts = (Long) parsedPk[1];

                        // Use the JSON timestamp if available, otherwise use the event timestamp
                        if (ts == null) {
                            ts = timestamp;
                        }
                        
                        // Create a Row with the primary key and timestamp
                        allRows.add(RowFactory.create(pk_id, ts));
                        fileRowCount.incrementAndGet();
                        return true;
                    });
                }
                
                totalProcessed += fileRowCount.get();
                logger.info("Processed {} rows from file: {}", fileRowCount.get(), new File(filePath).getName());
            } catch (IOException e) {
                logger.error("Error processing file {}: {}", filePath, e.getMessage());
                throw e;
            }
        }


        logger.info("Total rows processed from all files: {}", totalProcessed);

        // Create a DataFrame from the collected rows
        if (allRows.isEmpty()) {
            // Return an empty DataFrame with the correct schema
            // Use Java collections empty list instead of empty RDD
            return spark.createDataFrame(
                    Collections.emptyList(),
                    schema
            );
        }

        // Create the DataFrame directly from the collected rows
        return spark.createDataFrame(allRows, schema);
    }
    
    /**
     * Creates a DataFrame directly from in-memory data structures without using RDD.
     * This is optimized for small files where the overhead of distributed processing is not needed.
     * This overload uses LongType as the default data type for primary keys.
     * 
     * @param filePaths List of paths to delta log files
     * @param spark The SparkSession to use
     * @return A DataFrame with pk and ts columns
     * @throws IOException If an I/O error occurs
     */
    public static Dataset<Row> createDataFrame(List<String> filePaths, SparkSession spark) throws IOException {
        return createDataFrame(filePaths, spark, DataTypes.LongType);
    }
} 