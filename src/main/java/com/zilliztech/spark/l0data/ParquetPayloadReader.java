package com.zilliztech.spark.l0data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * ParquetPayloadReader reads and parses parquet format data from a byte array.
 * This implementation aligns with the Go implementation's ParquetPayloadReader.
 */
public class ParquetPayloadReader {
    private static final Logger logger = LoggerFactory.getLogger(ParquetPayloadReader.class);
    
    private static final String PK_COLUMN_NAME = "pk";
    private static final String TS_COLUMN_NAME = "ts";

    private final DataType dataType;
    private final byte[] data;
    private final Configuration hadoopConfig;
    private File tempFile;
    private ParquetFileReader reader;
    private MessageType schema;
    
    /**
     * Creates a new ParquetPayloadReader.
     *
     * @param dataType The data type of the primary keys
     * @param data The parquet format data as a byte array
     */
    public ParquetPayloadReader(DataType dataType, byte[] data) {
        this.dataType = dataType;
        this.data = data;
        this.hadoopConfig = new Configuration();
    }
    
    /**
     * Initializes the Parquet reader.
     * This method creates a temporary file to store the Parquet data,
     * since Parquet libraries work with files rather than byte arrays.
     *
     * @throws IOException If an I/O error occurs
     */
    private void initializeReader() throws IOException {
        if (reader != null) {
            return; // Already initialized
        }
        
        if (data.length == 0) {
            throw new IOException("Empty Parquet data");
        }
        
        try {
            // Create a temporary file to store the Parquet data
            tempFile = File.createTempFile("parquet_payload_", ".parquet");
            tempFile.deleteOnExit();
            
            // Write the Parquet data to the temporary file
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                fos.write(data);
            }
            
            logger.debug("Created temporary Parquet file at {} with size {} bytes", 
                        tempFile.getAbsolutePath(), data.length);
            
            // Open the Parquet file
            reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(tempFile.getAbsolutePath()), hadoopConfig));
            schema = reader.getFooter().getFileMetaData().getSchema();
            
            logger.debug("Parquet schema: {}", schema);
            
            // Print field names for debugging
            List<Type> fields = schema.getFields();
            if (fields.isEmpty()) {
                logger.warn("Parquet schema has no fields");
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Schema field names:");
                    for (int i = 0; i < fields.size(); i++) {
                        Type field = fields.get(i);
                        logger.debug("  Field {}: {} ({})", i, field.getName(), 
                                    field.asPrimitiveType().getPrimitiveTypeName());
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Error initializing Parquet reader: {}", e.getMessage());
            e.printStackTrace();
            cleanupTempFile();
            throw new IOException("Failed to initialize Parquet reader: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error parsing Parquet: {}", e.getMessage());
            e.printStackTrace();
            cleanupTempFile();
            throw new IOException("Unexpected error parsing Parquet: " + e.getMessage(), e);
        }
    }
    
    /**
     * Cleans up temporary files created during the reading process.
     */
    private void cleanupTempFile() {
        if (tempFile != null && tempFile.exists()) {
            try {
                Files.delete(tempFile.toPath());
            } catch (IOException e) {
                // Log but continue - this is just cleanup
                logger.warn("Failed to delete temporary file: {}", e.getMessage());
            }
            tempFile = null;
        }
    }
    
    /**
     * Closes the reader and cleans up resources.
     */
    public void close() {
        if (reader != null) {
            try {
                reader.close();
                reader = null;
            } catch (IOException e) {
                // Log but continue - this is just cleanup
                logger.warn("Failed to close Parquet reader: {}", e.getMessage());
            }
        }
        
        cleanupTempFile();
    }
    
    /**
     * Gets boolean values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of boolean values
     * @throws IOException If an I/O error occurs
     */
    public List<Boolean> getBooleanFromPayload(int columnIndex) throws IOException {
        List<Boolean> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.BOOLEAN)) {
                values.add(group.getBoolean(columnIndex, 0));
            }
        });
        return values;
    }
    
    /**
     * Gets int8 (byte) values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of byte values
     * @throws IOException If an I/O error occurs
     */
    public List<Byte> getInt8FromPayload(int columnIndex) throws IOException {
        List<Byte> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.INT32)) {
                values.add((byte) group.getInteger(columnIndex, 0));
            }
        });
        return values;
    }
    
    /**
     * Gets int16 (short) values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of short values
     * @throws IOException If an I/O error occurs
     */
    public List<Short> getInt16FromPayload(int columnIndex) throws IOException {
        List<Short> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.INT32)) {
                values.add((short) group.getInteger(columnIndex, 0));
            }
        });
        return values;
    }
    
    /**
     * Gets int32 (integer) values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of integer values
     * @throws IOException If an I/O error occurs
     */
    public List<Integer> getInt32FromPayload(int columnIndex) throws IOException {
        List<Integer> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.INT32)) {
                values.add(group.getInteger(columnIndex, 0));
            }
        });
        return values;
    }
    
    /**
     * Gets int64 (long) values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of long values
     * @throws IOException If an I/O error occurs
     */
    public List<Long> getInt64FromPayload(int columnIndex) throws IOException {
        List<Long> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            // Try to get values even if the column type doesn't exactly match
            // This helps with schema variations
            if (schema.getFieldCount() > 0 && columnIndex < schema.getFieldCount()) {
                Type fieldType = schema.getType(columnIndex);
                
                // Handle case where we only have one column and it might be our target
                if (schema.getFieldCount() == 1 && columnIndex == 0) {
                    try {
                        if (fieldType.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64) {
                            if (group.getFieldRepetitionCount(0) > 0) {
                                values.add(group.getLong(0, 0));
                            }
                        } else if (fieldType.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
                            // Try to parse the string as a number
                            try {
                                String strValue = group.getString(0, 0);
                                values.add(Long.parseLong(strValue));
                            } catch (NumberFormatException e) {
                                // Not a number, ignore
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Error accessing column 0: {}", e.getMessage());
                    }
                } 
                // Normal case - try INT64 column
                else if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.INT64)) {
                    values.add(group.getLong(columnIndex, 0));
                }
            }
        });
        return values;
    }
    
    /**
     * Gets float32 (float) values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of float values
     * @throws IOException If an I/O error occurs
     */
    public List<Float> getFloat32FromPayload(int columnIndex) throws IOException {
        List<Float> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.FLOAT)) {
                values.add(group.getFloat(columnIndex, 0));
            }
        });
        return values;
    }
    
    /**
     * Gets float64 (double) values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of double values
     * @throws IOException If an I/O error occurs
     */
    public List<Double> getFloat64FromPayload(int columnIndex) throws IOException {
        List<Double> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.DOUBLE)) {
                values.add(group.getDouble(columnIndex, 0));
            }
        });
        return values;
    }
    
    /**
     * Gets string values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of string values
     * @throws IOException If an I/O error occurs
     */
    public List<String> getStringFromPayload(int columnIndex) throws IOException {
        List<String> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            // Try to get values even if the column type doesn't exactly match
            // This helps with schema variations
            if (schema.getFieldCount() > 0) {
                // If we only have one column, use it regardless of index
                if (schema.getFieldCount() == 1 && columnIndex < schema.getFieldCount()) {
                    try {
                        if (group.getFieldRepetitionCount(0) > 0) {
                            Type fieldType = schema.getType(0);
                            if (fieldType.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
                                values.add(group.getString(0, 0));
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Error accessing single column: {}", e.getMessage());
                    }
                }
                // Normal case - try the specified column index
                else if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.BINARY)) {
                    values.add(group.getString(columnIndex, 0));
                }
            }
        });
        return values;
    }
    
    /**
     * Gets binary values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of binary values as byte arrays
     * @throws IOException If an I/O error occurs
     */
    public List<byte[]> getBinaryFromPayload(int columnIndex) throws IOException {
        List<byte[]> values = new ArrayList<>();
        processParquetFile((group, schema) -> {
            if (isValidColumnAccess(group, schema, columnIndex, PrimitiveType.PrimitiveTypeName.BINARY)) {
                ByteBuffer buffer = group.getBinary(columnIndex, 0).toByteBuffer();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                values.add(bytes);
            }
        });
        return values;
    }
    
    /**
     * Gets float vector values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of float vectors (as float arrays)
     * @throws IOException If an I/O error occurs
     */
    public List<float[]> getFloatVectorFromPayload(int columnIndex) throws IOException {
        // Note: This implementation assumes that the float vector is stored as a LIST of FLOATs
        List<float[]> values = new ArrayList<>();
        // This requires custom implementation based on the specific Parquet schema used
        // TODO: Implement for specific use cases as needed
        return values;
    }
    
    /**
     * Gets binary vector values from the payload for the specified column.
     *
     * @param columnIndex The column index
     * @return List of binary vectors (as byte arrays)
     * @throws IOException If an I/O error occurs
     */
    public List<byte[]> getBinaryVectorFromPayload(int columnIndex) throws IOException {
        // Note: This implementation assumes that the binary vector is stored as a BINARY field
        return getBinaryFromPayload(columnIndex);
    }
    
    /**
     * Checks if a column access is valid for the given schema and group.
     * 
     * @param group The group to check
     * @param schema The schema to check against
     * @param columnIndex The column index to access
     * @param expectedType The expected primitive type
     * @return True if the access is valid
     */
    private boolean isValidColumnAccess(Group group, MessageType schema, int columnIndex, 
                                        PrimitiveType.PrimitiveTypeName expectedType) {
        if (columnIndex >= schema.getFieldCount()) {
            return false;
        }
        
        try {
            if (group.getFieldRepetitionCount(columnIndex) <= 0) {
                return false;
            }
            
            Type field = schema.getType(columnIndex);
            if (!field.isPrimitive()) {
                return false;
            }
            
            // Be more permissive about types - allow access if the basic category matches
            PrimitiveType.PrimitiveTypeName actualType = field.asPrimitiveType().getPrimitiveTypeName();
            
            // For numeric columns, allow some flexibility - integer types can be cast
            if (expectedType == PrimitiveType.PrimitiveTypeName.INT32 ||
                expectedType == PrimitiveType.PrimitiveTypeName.INT64) {
                return actualType == PrimitiveType.PrimitiveTypeName.INT32 ||
                       actualType == PrimitiveType.PrimitiveTypeName.INT64;
            }
            
            return actualType == expectedType;
        } catch (Exception e) {
            logger.error("Error checking column access: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Processes the Parquet file and applies the specified record processor.
     *
     * @param processor The record processor to apply to each Parquet record
     * @throws IOException If an I/O error occurs
     */
    private void processParquetFile(RecordProcessor processor) throws IOException {
        try {
            initializeReader();
            
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(
                    pages, new GroupRecordConverter(schema));
                    
                for (int i = 0; i < rows; i++) {
                    Group group = recordReader.read();
                    processor.process(group, schema);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Parquet file: {}", e.getMessage());
            e.printStackTrace();
        } finally {
            close();
        }
    }
    
    /**
     * Functional interface for processing Parquet records.
     */
    @FunctionalInterface
    private interface RecordProcessor {
        void process(Group group, MessageType schema) throws IOException;
    }
} 