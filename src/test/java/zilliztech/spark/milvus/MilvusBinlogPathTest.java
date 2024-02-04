package zilliztech.spark.milvus;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class MilvusBinlogPathTest {

    @Test
    public void parseS3Path() throws Exception {
        String backupPath = "/zilliz-aws-us-west-2-abcd/backup";
        URI path = null;
        try {
            path = new URI(backupPath);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        System.out.println(path);
    }
}
