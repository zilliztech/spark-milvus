package com.zilliz.spark.connector

import org.apache.hadoop.conf.Configuration

/** The target role shared by Hadoop COS access and the native storage client.
  */
private[connector] object TencentStorageAuth {
  val CredentialsProvider =
    "com.zilliz.cloud.hadoop.TencentS3RoleCredentialsProvider"

  final case class Role(
      arn: String,
      sessionName: Option[String],
      externalId: Option[String]
  )

  def resolve(conf: Configuration, bucket: String = ""): Option[Role] = {
    def setting(suffix: String): Option[String] = {
      val bucketValue = if (bucket.nonEmpty) {
        Option(conf.getTrimmed(s"fs.s3a.bucket.$bucket.$suffix"))
      } else None
      // An explicitly empty bucket value must not inherit a different identity.
      bucketValue
        .orElse(Option(conf.getTrimmed(s"fs.s3a.$suffix")))
        .filter(_.nonEmpty)
    }

    val providers = setting("aws.credentials.provider").toSeq
      .flatMap(_.split(','))
      .map(_.trim)
    if (!providers.contains(CredentialsProvider)) return None
    require(
      providers == Seq(CredentialsProvider),
      "Tencent S3A role provider cannot be mixed with another credentials provider"
    )
    val arn = setting("assumed.role.arn").getOrElse {
      throw new IllegalArgumentException(
        s"Tencent S3A role provider requires assumed.role.arn for bucket '$bucket'"
      )
    }
    require(
      arn.matches(
        "qcs::cam::uin/[0-9]+:(roleName/[A-Za-z0-9_+=,.@-]+|role/[0-9]+)"
      ),
      "Tencent S3A role provider requires a Tencent CAM role ARN"
    )
    Some(
      Role(
        arn,
        setting("assumed.role.session.name"),
        setting("assumed.role.external.id")
      )
    )
  }
}
