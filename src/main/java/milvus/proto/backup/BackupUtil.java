package milvus.proto.backup;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import software.amazon.awssdk.services.s3.S3Client;

public class BackupUtil {

    public final static String META = "/meta/";
    public final static String FULL_META = "full_meta.json";

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
