package com.zilliz.spark.connector.options

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties

/** The mapping table is storage-access.html 3.3; these pin it. */
class HadoopStorageKeysTest extends AnyFunSuite with Matchers {

  private def conf(pairs: (String, String)*): Configuration = {
    val c = new Configuration(false)
    pairs.foreach { case (k, v) => c.set(k, v) }
    c
  }

  test("s3a AssumeRole keys become fs.* and imply the aws provider") {
    val out = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/data",
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com",
        "fs.s3a.endpoint.region" -> "us-west-2"
      )
    )
    out(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/data"
    out(StorageProperties.Address) shouldBe "s3.us-west-2.amazonaws.com"
    out(StorageProperties.Region) shouldBe "us-west-2"
    out(StorageProperties.CloudProvider) shouldBe "aws"
  }

  test("s3a static keys translate") {
    val out = HadoopStorageKeys.toFsProperties(
      conf("fs.s3a.access.key" -> "ak", "fs.s3a.secret.key" -> "sk")
    )
    out(StorageProperties.AccessKeyId) shouldBe "ak"
    out(StorageProperties.AccessKeyValue) shouldBe "sk"
    out(StorageProperties.CloudProvider) shouldBe "aws"
  }

  test("oss keys become fs.* and imply the aliyun provider") {
    val out = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.oss.accessKeyId" -> "id",
        "fs.oss.accessKeySecret" -> "secret",
        "fs.oss.endpoint" -> "oss-cn-hangzhou.aliyuncs.com"
      )
    )
    out(StorageProperties.AccessKeyId) shouldBe "id"
    out(StorageProperties.AccessKeyValue) shouldBe "secret"
    out(StorageProperties.Address) shouldBe "oss-cn-hangzhou.aliyuncs.com"
    out(StorageProperties.CloudProvider) shouldBe "aliyun"
  }

  test("no storage keys yields an empty map, safe to merge") {
    HadoopStorageKeys.toFsProperties(
      conf("spark.sql.shuffle.partitions" -> "200")
    ) shouldBe empty
  }

  test("blank values are dropped, not forwarded as empty") {
    HadoopStorageKeys.toFsProperties(
      conf("fs.s3a.assumed.role.arn" -> "   ")
    ) shouldBe empty
  }

  test("both namespaces present leaves the provider unset") {
    // A misconfiguration: let the C layer's validation report it rather than
    // guess a provider.
    val out = HadoopStorageKeys.toFsProperties(
      conf("fs.s3a.access.key" -> "ak", "fs.oss.accessKeyId" -> "id")
    )
    out should not contain key(StorageProperties.CloudProvider)
  }

  // Backfill writes its real configuration under fs.s3a.bucket.<name>.*, so a
  // translator that reads only the global prefix drops the endpoint and the
  // credentials that actually apply.
  test("a per-bucket key beats the global one") {
    val c = conf(
      "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com",
      "fs.s3a.access.key" -> "global-ak",
      "fs.s3a.secret.key" -> "global-sk",
      "fs.s3a.bucket.backfill-src.endpoint" -> "http://minio:9000",
      "fs.s3a.bucket.backfill-src.access.key" -> "bucket-ak",
      "fs.s3a.bucket.backfill-src.secret.key" -> "bucket-sk"
    )

    val out = HadoopStorageKeys.toFsProperties(c, "backfill-src")
    out(StorageProperties.Address) shouldBe "http://minio:9000"
    out(StorageProperties.AccessKeyId) shouldBe "bucket-ak"
    out(StorageProperties.AccessKeyValue) shouldBe "bucket-sk"

    // Another bucket in the same session still gets the global values.
    val other = HadoopStorageKeys.toFsProperties(c, "milvus-storage")
    other(StorageProperties.Address) shouldBe "s3.us-west-2.amazonaws.com"
    other(StorageProperties.AccessKeyId) shouldBe "global-ak"
  }

  test("a per-bucket key is read when no global one exists") {
    val out = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.bucket.only-here.endpoint" -> "http://minio:9000",
        "fs.s3a.bucket.only-here.access.key" -> "ak",
        "fs.s3a.bucket.only-here.secret.key" -> "sk"
      ),
      "only-here"
    )
    out(StorageProperties.Address) shouldBe "http://minio:9000"
    out(StorageProperties.AccessKeyId) shouldBe "ak"
    out(StorageProperties.CloudProvider) shouldBe "aws"
  }

  // The consequence of missing them: with no access key in sight the IAM
  // fallback fires, and a deployment that configured static credentials
  // silently authenticates as the pod instead of failing.
  test("per-bucket static keys keep the IAM fallback from firing") {
    val props = StorageProperties.from(
      HadoopStorageKeys.toFsProperties(
        conf(
          "fs.s3a.bucket.b1.endpoint" -> "http://minio:9000",
          "fs.s3a.bucket.b1.access.key" -> "ak",
          "fs.s3a.bucket.b1.secret.key" -> "sk"
        ),
        "b1"
      ) + (StorageProperties.BucketName -> "b1")
    )
    props(StorageProperties.AccessKeyId) shouldBe "ak"
    props.get(StorageProperties.UseIam) should not be Some("true")
  }

  test("a per-bucket assumed role beats the global one") {
    val out = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/global",
        "fs.s3a.bucket.b1.assumed.role.arn" -> "arn:aws:iam::1:role/bucket",
        "fs.s3a.bucket.b1.assumed.role.external.id" -> "ext-1"
      ),
      "b1"
    )
    out(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/bucket"
    out(StorageProperties.ExternalId) shouldBe "ext-1"
  }

  test("oss per-bucket keys resolve the same way") {
    val out = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.oss.endpoint" -> "oss-cn-hangzhou.aliyuncs.com",
        "fs.oss.bucket.b1.endpoint" -> "oss-cn-beijing.aliyuncs.com",
        "fs.oss.bucket.b1.accessKeyId" -> "id",
        "fs.oss.bucket.b1.accessKeySecret" -> "secret"
      ),
      "b1"
    )
    out(StorageProperties.Address) shouldBe "oss-cn-beijing.aliyuncs.com"
    out(StorageProperties.AccessKeyId) shouldBe "id"
    out(StorageProperties.CloudProvider) shouldBe "aliyun"
  }

  test("the shim output feeds StorageProperties end to end") {
    val fs = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com",
        "fs.s3a.access.key" -> "ak",
        "fs.s3a.secret.key" -> "sk"
      )
    ) + (StorageProperties.BucketName -> "milvus-bucket")

    val props = StorageProperties.from(fs)
    props(StorageProperties.CloudProvider) shouldBe "aws"
    props(StorageProperties.AccessKeyId) shouldBe "ak"
    props(StorageProperties.BucketName) shouldBe "milvus-bucket"
  }

  private val AssumedRole =
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
  private val Simple = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

  // The native side has to use the identity Hadoop would use for the bucket. A
  // per-bucket Simple provider with static keys does not assume the globally
  // configured role.
  test(
    "a per-bucket Simple provider with static keys does not inherit the global role"
  ) {
    val out = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.aws.credentials.provider" -> AssumedRole,
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/global",
        "fs.s3a.bucket.b.aws.credentials.provider" -> Simple,
        "fs.s3a.bucket.b.endpoint" -> "minio:9000",
        "fs.s3a.bucket.b.access.key" -> "ak",
        "fs.s3a.bucket.b.secret.key" -> "sk"
      ),
      "b"
    )
    out should not contain key(StorageProperties.RoleArn)
    val props =
      StorageProperties.from(out + (StorageProperties.BucketName -> "b"))
    props(StorageProperties.AccessKeyId) shouldBe "ak"
    props.get(StorageProperties.UseIam) should not be Some("true")
  }

  test(
    "an effective AssumedRole provider keeps its role; a provider naming neither drops both"
  ) {
    val bucketRole = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.aws.credentials.provider" -> Simple,
        "fs.s3a.access.key" -> "ak",
        "fs.s3a.secret.key" -> "sk",
        "fs.s3a.bucket.b.aws.credentials.provider" -> AssumedRole,
        "fs.s3a.bucket.b.assumed.role.arn" -> "arn:aws:iam::1:role/bucket",
        // As the managed platform sets it: the environment's identity signs
        // AssumeRole, so the inherited static keys play no part.
        "fs.s3a.bucket.b.assumed.role.credentials.provider" ->
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      ),
      "b"
    )
    bucketRole(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/bucket"

    val global = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.aws.credentials.provider" -> AssumedRole,
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/global"
      ),
      "b"
    )
    global(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/global"

    val staticGlobal = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.aws.credentials.provider" -> Simple,
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/unused",
        "fs.s3a.access.key" -> "ak",
        "fs.s3a.secret.key" -> "sk"
      )
    )
    staticGlobal should not contain key(StorageProperties.RoleArn)
    staticGlobal(StorageProperties.AccessKeyId) shouldBe "ak"

    val instance = HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.aws.credentials.provider" ->
          "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
        "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/unused",
        "fs.s3a.access.key" -> "ak",
        "fs.s3a.secret.key" -> "sk",
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"
      )
    )
    instance should not contain key(StorageProperties.RoleArn)
    instance should not contain key(StorageProperties.AccessKeyId)
  }

  // Hadoop's TLS switch reaches fs.use_ssl, per-bucket first; unset means
  // Hadoop's default, TLS on (decided 2026-09-16), unless the endpoint names
  // its own scheme.
  test(
    "Hadoop's TLS switch reaches fs.use_ssl, per bucket first, TLS by default"
  ) {
    HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.endpoint" -> "minio.example:443",
        "fs.s3a.access.key" -> "ak",
        "fs.s3a.secret.key" -> "sk",
        "fs.s3a.connection.ssl.enabled" -> "false",
        "fs.s3a.bucket.b1.connection.ssl.enabled" -> "true"
      ),
      "b1"
    )(StorageProperties.UseSSL) shouldBe "true"

    HadoopStorageKeys.toFsProperties(
      conf("fs.s3a.endpoint" -> "minio.example:443"),
      "b1"
    )(StorageProperties.UseSSL) shouldBe "true"

    HadoopStorageKeys.toFsProperties(
      conf("fs.s3a.endpoint" -> "http://minio:9000"),
      "b1"
    )(StorageProperties.UseSSL) shouldBe "false"

    HadoopStorageKeys.toFsProperties(
      conf(
        "fs.oss.endpoint" -> "oss-cn-hangzhou.aliyuncs.com",
        "fs.oss.connection.secure.enabled" -> "false"
      ),
      "b1"
    )(StorageProperties.UseSSL) shouldBe "false"

    HadoopStorageKeys.toFsProperties(
      conf(
        "fs.s3a.endpoint" -> "minio:9000",
        "fs.s3a.path.style.access" -> "true"
      ),
      "b1"
    )(StorageProperties.UseVirtualHost) shouldBe "false"
  }

  test("Hadoop's shipped defaults alone translate to nothing") {
    // core-default.xml sets connection.ssl.enabled, path.style.access and a
    // provider chain; none of that is a storage configuration.
    HadoopStorageKeys.toFsProperties(new Configuration(), "b1") shouldBe empty
  }

  test("an explicit fs.use_ssl option wins over the Hadoop value") {
    val c = conf(
      "fs.s3a.endpoint" -> "minio.example:443",
      "fs.s3a.connection.ssl.enabled" -> "true",
      "fs.s3a.access.key" -> "ak",
      "fs.s3a.secret.key" -> "sk"
    )
    StorageOptions.storagePropertiesFor(c, "b1", Map.empty)(
      StorageProperties.UseSSL
    ) shouldBe "true"
    StorageOptions.storagePropertiesFor(
      c,
      "b1",
      Map(StorageProperties.UseSSL -> "false")
    )(StorageProperties.UseSSL) shouldBe "false"
  }

  // A temporary credential is a key pair plus a session token, and the native
  // layer takes no session token (storage-access.html 3.7).
  private val temporary = Seq(
    "fs.s3a.access.key" -> "ASIATEMP",
    "fs.s3a.secret.key" -> "sk",
    "fs.s3a.session.token" -> "token"
  )

  test(
    "a temporary key pair the native layer cannot use fails, naming the fix"
  ) {
    val e = intercept[IllegalArgumentException](
      HadoopStorageKeys.toFsProperties(conf(temporary: _*), "b1")
    )
    e.getMessage should include("fs.s3a.session.token")
    e.getMessage should include(StorageProperties.UseIam)
    // Hadoop's shipped chain starts with the temporary-credential provider.
    val withDefaults = new Configuration()
    temporary.foreach { case (k, v) => withDefaults.set(k, v) }
    intercept[IllegalArgumentException](
      HadoopStorageKeys.toFsProperties(withDefaults, "b1")
    )
    intercept[IllegalArgumentException](
      HadoopStorageKeys.toFsProperties(
        conf(
          "fs.s3a.access.key" -> "ASIATEMP",
          "fs.s3a.secret.key" -> "sk",
          "fs.s3a.bucket.b1.session.token" -> "token"
        ),
        "b1"
      )
    )
  }

  test("a session token does not matter to a chain of static keys only") {
    val out = HadoopStorageKeys.toFsProperties(
      conf(
        temporary :+ ("fs.s3a.aws.credentials.provider" ->
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"): _*
      ),
      "b1"
    )
    out(StorageProperties.AccessKeyId) shouldBe "ASIATEMP"
  }

  test(
    "temporary keys Spark copied from this process's environment go to the default chain"
  ) {
    val environment = Map(
      "AWS_ACCESS_KEY_ID" -> "ASIATEMP",
      "AWS_SECRET_ACCESS_KEY" -> "sk",
      "AWS_SESSION_TOKEN" -> "token"
    )
    val c = conf(
      temporary :+ ("fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"): _*
    )
    val out = HadoopStorageKeys.toFsProperties(c, "b1", environment)
    out should not contain key(StorageProperties.AccessKeyId)
    out should not contain key(StorageProperties.AccessKeyValue)
    out(StorageProperties.Address) shouldBe "s3.us-west-2.amazonaws.com"
    HadoopStorageKeys.canonicalProperties(
      out + (StorageProperties.BucketName -> "b1")
    )(StorageProperties.UseIam) shouldBe "true"

    // Another token in the environment is another credential.
    intercept[IllegalArgumentException](
      HadoopStorageKeys.toFsProperties(
        c,
        "b1",
        environment + ("AWS_SESSION_TOKEN" -> "other")
      )
    )
  }

  // Hadoop tries a provider chain in order and uses the first provider that
  // yields credentials. The native layer takes one identity: static keys, its
  // default chain, or a role assumed with its default chain. A chain whose
  // outcome the connector cannot express that way is refused, not guessed.
  private val Env =
    "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider"
  private val DefaultChainV1 =
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
  private val roleAndKeys = Seq(
    "fs.s3a.access.key" -> "ak",
    "fs.s3a.secret.key" -> "sk",
    "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/r",
    "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"
  )
  private def translated(pairs: (String, String)*): Map[String, String] =
    HadoopStorageKeys.canonicalProperties(
      HadoopStorageKeys.toFsProperties(conf(pairs: _*), "b") +
        (StorageProperties.BucketName -> "b")
    )

  test(
    "static keys ahead of a role: Hadoop uses the keys, so does the connector"
  ) {
    val out = translated(
      roleAndKeys :+ ("fs.s3a.aws.credentials.provider" -> s"$Simple,$AssumedRole"): _*
    )
    out should not contain key(StorageProperties.RoleArn)
    out(StorageProperties.AccessKeyId) shouldBe "ak"
  }

  test("a chain that mixes a role with another identity source is refused") {
    Seq(s"$AssumedRole,$Simple", s"$AssumedRole,$Env", s"$Env,$AssumedRole")
      .foreach { chain =>
        withClue(chain) {
          val e = intercept[IllegalArgumentException](
            translated(
              roleAndKeys :+ ("fs.s3a.aws.credentials.provider" -> chain): _*
            )
          )
          e.getMessage should include(StorageProperties.RoleArn)
          e.getMessage should include(StorageProperties.UseIam)
        }
      }
  }

  test(
    "an environment provider ahead of static keys is refused when keys are set"
  ) {
    intercept[IllegalArgumentException](
      translated(
        "fs.s3a.aws.credentials.provider" -> s"$Env,$Simple",
        "fs.s3a.access.key" -> "ak",
        "fs.s3a.secret.key" -> "sk",
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"
      )
    )
    // Without keys the static provider never applies: the environment's
    // identity, which the native default chain also resolves.
    translated(
      "fs.s3a.aws.credentials.provider" -> s"$Env,$Simple",
      "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"
    )(StorageProperties.UseIam) shouldBe "true"
  }

  test("a role whose STS call Hadoop signs with the static keys is refused") {
    // Hadoop's default fs.s3a.assumed.role.credentials.provider is Simple, so
    // the keys sign AssumeRole; the native layer can only sign it with its
    // default chain.
    intercept[IllegalArgumentException](
      translated(
        roleAndKeys :+ ("fs.s3a.aws.credentials.provider" -> AssumedRole): _*
      )
    ).getMessage should include("assumed.role.credentials.provider")
    // Signed by the environment's identity (the managed platform's setting):
    // the role is kept and the keys play no part.
    val platform = translated(
      roleAndKeys ++ Seq(
        "fs.s3a.aws.credentials.provider" -> AssumedRole,
        "fs.s3a.assumed.role.credentials.provider" -> DefaultChainV1
      ): _*
    )
    platform(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/r"
    platform should not contain key(StorageProperties.AccessKeyId)
  }

  test("a provider class the connector cannot map is refused") {
    intercept[IllegalArgumentException](
      translated(
        "fs.s3a.aws.credentials.provider" -> "com.example.VaultCredentialsProvider",
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"
      )
    ).getMessage should include("com.example.VaultCredentialsProvider")
  }

  test("an OSS provider list is refused, as hadoop-aliyun takes one class") {
    intercept[IllegalArgumentException](
      HadoopStorageKeys.toFsProperties(
        conf(
          "fs.oss.credentials.provider" ->
            "com.zilliz.cloud.hadoop.AliyunOSSRoleCredentialsProvider,org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider",
          "fs.oss.assumed.role.arn" -> "acs:ram::1:role/r",
          "fs.oss.endpoint" -> "oss-cn-hangzhou.aliyuncs.com"
        ),
        "b"
      )
    )
  }

  test("with no provider configured, a role together with keys is refused") {
    intercept[IllegalArgumentException](translated(roleAndKeys: _*))
  }

  test(
    "options that name the identity win over a chain that would be refused"
  ) {
    val refused = conf(
      "fs.s3a.aws.credentials.provider" -> s"$Env,$AssumedRole",
      "fs.s3a.assumed.role.arn" -> "arn:aws:iam::1:role/session"
    )
    val base = Map(
      StorageProperties.BucketName -> "b",
      StorageProperties.Address -> "s3.us-west-2.amazonaws.com"
    )
    val withKeys = StorageOptions.storagePropertiesFor(
      refused,
      "b",
      base ++ Map(
        StorageProperties.AccessKeyId -> "ak",
        StorageProperties.AccessKeyValue -> "sk"
      )
    )
    withKeys(StorageProperties.AccessKeyId) shouldBe "ak"
    withKeys should not contain key(StorageProperties.RoleArn)
    val withRole = StorageOptions.storagePropertiesFor(
      refused,
      "b",
      base + (StorageProperties.RoleArn -> "arn:aws:iam::1:role/declared")
    )
    withRole(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/declared"
    intercept[IllegalArgumentException](
      StorageOptions.storagePropertiesFor(refused, "b", base)
    )
  }

  test("an OSS chain the connector cannot map does not stop an S3 bucket") {
    val c = conf(
      "fs.oss.credentials.provider" ->
        "com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider",
      "fs.s3a.access.key" -> "ak",
      "fs.s3a.secret.key" -> "sk",
      "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"
    )
    HadoopStorageKeys.toFsProperties(c, "b")(
      StorageProperties.AccessKeyId
    ) shouldBe "ak"
    // For an OSS bucket that chain is the one Hadoop uses, and it is judged.
    intercept[IllegalArgumentException](
      HadoopStorageKeys.toFsProperties(
        c,
        "b",
        declared = Map(StorageProperties.CloudProvider -> "aliyun")
      )
    )
  }

  test("keys from this process's environment may sign AssumeRole") {
    // The native default chain signs with the same variables Spark copied.
    val environment = Map(
      "AWS_ACCESS_KEY_ID" -> "ak",
      "AWS_SECRET_ACCESS_KEY" -> "sk"
    )
    val c = conf(
      roleAndKeys :+ ("fs.s3a.aws.credentials.provider" -> AssumedRole): _*
    )
    val out = HadoopStorageKeys.toFsProperties(c, "b", environment)
    out(StorageProperties.RoleArn) shouldBe "arn:aws:iam::1:role/r"
    out should not contain key(StorageProperties.AccessKeyId)
    HadoopStorageKeys.s3aAssumesRole(c, "b", environment) shouldBe true
    intercept[IllegalArgumentException](
      HadoopStorageKeys.s3aAssumesRole(
        c,
        "b",
        environment + ("AWS_ACCESS_KEY_ID" -> "other")
      )
    )
  }
}
