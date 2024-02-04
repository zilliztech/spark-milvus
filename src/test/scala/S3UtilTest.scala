import milvus.proto.backup.S3Util
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

object S3UtilTest {
  def main(args: Array[String]): Unit = {
    val s3AK = ""
    val s3SK = ""
    val bucketName = ""
    val region = Region.US_WEST_2
    val s3Client = S3Client.builder.region(region).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AK, s3SK))).build
    val paths = S3Util.ListS3Json(s3Client, bucketName, "")
    println(paths)
  }
}
