package milvus.proto.backup;

// snippet-start:[s3.java2.getobjectdata.import]
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
// snippet-end:[s3.java2.getobjectdata.import]

/**
 * Before running this Java V2 code example, set up your development environment, including your credentials.
 *
 * For more information, see the following documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class S3Util {

    public static void main(String[] args) {

        final String usage = "\n" +
                "Usage:\n" +
                "    <bucketName> <keyName> <path>\n\n" +
                "Where:\n" +
                "    bucketName - The Amazon S3 bucket name. \n\n"+
                "    keyName - The key name. \n\n"+
                "    path - The path where the file is written to. \n\n";

        if (args.length != 3) {
            System.out.println(usage);
            System.exit(1);
        }

        String bucketName = args[0];
        String keyName = args[1];
        String path = args[2];

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_EAST_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        getObjectBytes(s3,bucketName,keyName);
    }

    // snippet-start:[s3.java2.getobjectdata.main]
    public static byte[] getObjectBytes (S3Client s3, String bucketName, String keyName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();
            return data;
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return null;
        }
    }

    public static boolean isExists(S3Client s3, String bucketName, String key) {
        ListObjectsV2Response result = s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(key).build());
        return !result.contents().isEmpty();
    }

    public static List<String> ListS3Json(S3Client s3, String bucketName, String key) {
        ListObjectsV2Response result = s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(key).build());
        List<String> arr = new ArrayList<>();
        for (int i = 0; i < result.contents().size(); i++) {
            if (result.contents().get(i).key().endsWith(".json")) {
                arr.add(result.contents().get(i).key());
            }
        }
        return arr;
    }

    public static List<String> ListLocal(String key) {
        File directory = new File(key);
        File[] result = directory.listFiles();
        List<String> arr = new ArrayList<>();
        for (int i = 0; i < result.length; i++) {
            if (!result[i].getName().startsWith(".") && !result[i].getName().startsWith("_")) {
                arr.add(result[i].getAbsolutePath());
            }
        }
        return arr;
    }
}
