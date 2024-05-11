package milvus.proto.backup;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import software.amazon.awssdk.services.s3.S3Client;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.OSSObject;

public class BackupUtil {

    public final static String META = "meta/";
    public final static String FULL_META = "full_meta.json";

    public static Backup.BackupInfo GetBackupInfoFromOSS(OSS ossClient, String bucketName, String backupPath) throws IOException {
        String fullMeta = backupPath + META + FULL_META;
//        byte[] buffer = S3Util.getObjectBytes(ossClient, bucketName, fullMeta);
        OSSObject ossObject = ossClient.getObject(bucketName, fullMeta);
        // 调用ossObject.getObjectContent获取文件输入流，可读取此输入流获取其内容。
        InputStream content = ossObject.getObjectContent();

//        byte[] bytes = new byte[0];
//        bytes = new byte[content.available()];
//        content.read(bytes);
//        String json = new String(bytes);
//
        StringBuffer bf = new StringBuffer();
        if (content != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(content));
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                bf.append(line);
            }
            // 数据读取完成后，获取的流必须关闭，否则会造成连接泄漏，导致请求无连接可用，程序无法正常工作。
            content.close();
        }

        String json = bf.toString();

        Backup.BackupInfo.Builder builder = Backup.BackupInfo.newBuilder();
        Backup.BackupInfo backupInfo;
        try {
            JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
            parser.merge(json, builder);
            backupInfo = builder.build();
            return backupInfo;
            // Use the deserialized message as needed
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    public static Backup.BackupInfo GetBackupInfoFromS3(S3Client s3, String bucketName, String backupPath) throws IOException {
        String fullMeta = backupPath + META + FULL_META;
        byte[] buffer = S3Util.getObjectBytes(s3, bucketName, fullMeta);
        String json = new String(buffer);

        Backup.BackupInfo.Builder builder = Backup.BackupInfo.newBuilder();
        Backup.BackupInfo backupInfo;
        try {
            JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
            parser.merge(json, builder);
            backupInfo = builder.build();
            return backupInfo;
            // Use the deserialized message as needed
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    public static Backup.BackupInfo GetBackupInfoFromLocal(String backupPath) throws IOException {
        String fullMeta = backupPath + META + FULL_META;

        File file = new File(fullMeta);
        FileInputStream fi = new FileInputStream(file);
        int fileLength = Integer.parseInt(String.valueOf(file.length()));
        byte[] buffer = new byte[fileLength];
        fi.read(buffer);
        String json = new String(buffer);

        Backup.BackupInfo.Builder builder = Backup.BackupInfo.newBuilder();
        Backup.BackupInfo backupInfo;

        System.out.println(com.google.protobuf.Descriptors.FileDescriptor.class.getClassLoader().toString());
        try {
            JsonFormat.Parser parser = JsonFormat.parser().ignoringUnknownFields();
            parser.merge(json, builder);
            backupInfo = builder.build();
            return backupInfo;
            // Use the deserialized message as needed
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    public static String getSchemeFromPath(String pathString) {
        try {
            URI uri = new URI(pathString);
            return uri.getScheme();
        } catch (URISyntaxException e) {
            // Handle the exception if the pathString is not a valid URI
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) throws IOException {
        String backupPath = "/Users/wanganyang/git_base/custom-spark-reader-example/data/mybackup2/";
        Backup.BackupInfo backupInfo = BackupUtil.GetBackupInfoFromLocal(backupPath);

    }
}
