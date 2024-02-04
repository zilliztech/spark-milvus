package zilliztech.spark.milvus;

import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MilvusBinlogParserTest {

    @Test
    public void parse() throws Exception {
        String fileName = "data/read_binlog_demo/vector_binlog";

        DataInputStream dataIn = new DataInputStream(new FileInputStream(fileName));
        Path path = Paths.get(fileName);
        // size of a file (in bytes)
        long bytes = Files.size(path);

        MilvusBinlogParser milvus = new MilvusBinlogParser(dataIn, bytes);
        milvus.parse();
        milvus.getInputStream();

        File targetFile = new File("data/read_binlog_demo/vector_binlog.parquet");
        OutputStream outStream = new FileOutputStream(targetFile);

        byte[] buffer = new byte[8 * 1024];
        int bytesRead;
        while ((bytesRead = milvus.getInputStream().read(buffer)) != -1) {
            outStream.write(buffer, 0, bytesRead);
        }

        outStream.close();
        dataIn.close();
    }
}
