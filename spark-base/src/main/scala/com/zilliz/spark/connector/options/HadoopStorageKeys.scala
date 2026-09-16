package com.zilliz.spark.connector.options

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}

/** Translates the Hadoop-style storage keys a runtime already has
  * (`spark.hadoop.fs.s3a.*`, `fs.oss.*`) into the `fs.*` keys
  * [[StorageProperties]] takes.
  *
  * This is the layer-3 shim described in storage-access.html 3.3. It exists so
  * an existing deployment — a managed platform that injects `fs.s3a.*`, or a
  * self-managed Spark configured the Hadoop way — keeps working without a
  * config change. The long-term shape is for whoever produces the configuration
  * to emit `fs.*` directly; this shim goes away with the last such deployment.
  *
  * The translation itself reads keys and renames them; it resolves no
  * credentials. [[objectStore]] is the one place that turns the result into a
  * live store, so every driver-side read opens storage the same way.
  *
  * Every entry point takes the bucket being opened, because a Hadoop key can be
  * set per bucket and that value wins over the global one.
  */
object HadoopStorageKeys extends Logging {

  private val S3A = "fs.s3a."
  private val OSS = "fs.oss."

  /** One Hadoop namespace: the key suffixes that make up a storage
    * configuration, grouped by what the effective credential provider decides
    * about them, and the provider classes that decide it.
    *
    * `sessionTokenSuffix` and `temporaryProviders` name a temporary credential:
    * the static keys plus a session token, used when the chain names one of
    * those providers. `environment` names the variables (key id, secret, token)
    * the platform's default credential chain reads, when Spark copies them into
    * this namespace.
    */
  private final case class Namespace(
      prefix: String,
      providerSuffix: String,
      location: Map[String, String],
      role: Map[String, String],
      staticKeys: Map[String, String],
      sessionTokenSuffix: String,
      temporaryProviders: Set[String],
      environment: Option[(String, String, String)],
      sslSuffix: String,
      pathStyleSuffix: Option[String],
      roleProviders: Set[String],
      staticKeyProviders: Set[String]
  )

  /** The class names Hadoop's S3A resolves an AssumeRole and static keys with.
    * BackfillConfig asks the same questions through [[namesAssumedRole]].
    */
  private[connector] val S3AAssumedRoleProvider =
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
  private[connector] val OssAssumedRoleProviders = Set(
    "com.zilliz.cloud.hadoop.AliyunOSSRoleCredentialsProvider",
    "org.apache.hadoop.fs.aliyun.oss.AssumedRoleCredentialProvider"
  )

  private val s3a = Namespace(
    prefix = S3A,
    providerSuffix = "aws.credentials.provider",
    location = Map(
      "endpoint" -> StorageProperties.Address,
      "endpoint.region" -> StorageProperties.Region
    ),
    role = Map(
      "assumed.role.arn" -> StorageProperties.RoleArn,
      "assumed.role.external.id" -> StorageProperties.ExternalId,
      "assumed.role.session.name" -> StorageProperties.SessionName
    ),
    staticKeys = Map(
      "access.key" -> StorageProperties.AccessKeyId,
      "secret.key" -> StorageProperties.AccessKeyValue
    ),
    sessionTokenSuffix = "session.token",
    temporaryProviders =
      Set("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"),
    // Spark copies these from the driver's environment into access.key,
    // secret.key and session.token (SparkHadoopUtil).
    environment = Some(
      ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    ),
    sslSuffix = "connection.ssl.enabled",
    pathStyleSuffix = Some("path.style.access"),
    roleProviders = Set(S3AAssumedRoleProvider),
    staticKeyProviders = Set(
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
      // AssumeRole signs its STS call with these keys when they are given.
      S3AAssumedRoleProvider
    )
  )

  private val oss = Namespace(
    prefix = OSS,
    providerSuffix = "credentials.provider",
    location = Map("endpoint" -> StorageProperties.Address),
    role = Map(
      "assumed.role.arn" -> StorageProperties.RoleArn,
      "assumed.role.external.id" -> StorageProperties.ExternalId,
      "assumed.role.session.name" -> StorageProperties.SessionName
    ),
    staticKeys = Map(
      "accessKeyId" -> StorageProperties.AccessKeyId,
      "accessKeySecret" -> StorageProperties.AccessKeyValue
    ),
    sessionTokenSuffix = "securityToken",
    // Hadoop's Aliyun provider uses the token whenever it is set.
    temporaryProviders =
      Set("org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider"),
    environment = None,
    sslSuffix = "connection.secure.enabled",
    pathStyleSuffix = None,
    roleProviders = OssAssumedRoleProviders,
    staticKeyProviders = Set(
      "org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider"
    ) ++ OssAssumedRoleProviders
  )

  /** The credential provider chain Hadoop uses for `bucket`: the per-bucket
    * key, else the global one; None when neither is set.
    */
  private[connector] def effectiveProvider(
      conf: Configuration,
      prefix: String,
      providerSuffix: String,
      bucket: String
  ): Option[String] = {
    def read(key: String): Option[String] =
      Option(conf.getTrimmed(key)).filter(_.nonEmpty)
    val trimmed = Option(bucket).map(_.trim).getOrElse("")
    (if (trimmed.isEmpty) None
     else read(s"${prefix}bucket.$trimmed.$providerSuffix"))
      .orElse(read(prefix + providerSuffix))
  }

  /** Whether a provider chain names one of `classes`. */
  private[connector] def names(chain: String, classes: Set[String]): Boolean =
    chain.split(',').map(_.trim).exists(classes.contains)

  private[connector] def namesAssumedRole(chain: String): Boolean =
    names(chain, Set(S3AAssumedRoleProvider) ++ OssAssumedRoleProviders)

  /** Reads the Hadoop keys for `bucket` out of `conf` and returns the `fs.*`
    * map.
    *
    * Hadoop lets a key be set once globally and again for one bucket
    * (`fs.s3a.bucket.<name>.access.key`), and the per-bucket value wins. A
    * deployment that talks to two buckets with different endpoints or different
    * credentials — backfill's source bucket and the Milvus storage bucket, for
    * one — has its real configuration only in the per-bucket keys, so reading
    * the global prefix alone picks up the wrong endpoint or no credentials at
    * all. `BackfillConfig.resolveAwsS3AssumeRole` resolves the same way.
    *
    * An empty `bucket` reads the global keys only.
    *
    * Empty when neither an `fs.s3a.*` nor an `fs.oss.*` key is present, so a
    * caller can merge it over user options unconditionally.
    */
  def toFsProperties(
      conf: Configuration,
      bucket: String = "",
      environment: Map[String, String] = sys.env
  ): Map[String, String] = {
    val s3a = translate(conf, this.s3a, bucket, environment)
    val oss = translate(conf, this.oss, bucket, environment)

    // Which backend runs is not a Hadoop key; Hadoop dispatches by scheme. Pick
    // the provider from whichever namespace supplied keys. Both present is a
    // misconfiguration, so leave cloud_provider unset and let the C layer's
    // validation speak.
    val provider =
      if (s3a.nonEmpty && oss.isEmpty)
        Some(StorageProperties.CloudProvider -> "aws")
      else if (oss.nonEmpty && s3a.isEmpty)
        Some(StorageProperties.CloudProvider -> "aliyun")
      else None

    val out = s3a ++ oss ++ provider
    out
  }

  /** Opens the store the driver reads snapshots, manifests, footers, delete
    * files and backup metadata through.
    *
    * An empty `bucket` means the caller has no object-storage location: a local
    * directory, or a path that names its own. That is `fs.storage_type=local`,
    * which is the C layer's local backend and needs neither an endpoint nor
    * credentials.
    *
    * Keys passed to the returned store are bucket-relative. The C filesystem
    * wraps its backend in a subtree rooted at `fs.bucket_name` and appends
    * whatever it is handed, so a fully qualified path arrives doubled.
    */
  def objectStore(conf: Configuration, bucket: String): ObjectStore = {
    val trimmed = Option(bucket).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) {
      return storeFrom(
        Map(StorageProperties.StorageType -> StorageProperties.StorageTypeLocal)
      )
    }
    storeFrom(
      toFsProperties(conf, trimmed) ++ Map(
        StorageProperties.BucketName -> trimmed
      )
    )
  }

  /** Opens a store from an `fs.*` map that a caller has already assembled.
    *
    * Validation and defaults are [[StorageProperties.from]]'s; the only thing
    * added here is the IAM fallback, which cannot live there because it is a
    * statement about Hadoop-shaped configuration, not about `fs.*`.
    */
  def storeFrom(properties: Map[String, String]): ObjectStore =
    NativeObjectStore
      .Factory(canonicalProperties(properties))
      .open()

  /** Validates one native property bag exactly as [[storeFrom]] does, without
    * opening a handle. Executor tasks use this so driver metadata reads and
    * executor segment reads cannot resolve aliases or IAM differently.
    */
  private[connector] def canonicalProperties(
      properties: Map[String, String]
  ): Map[String, String] =
    StorageProperties.from(properties ++ iamFallback(properties))

  /** A deployment that injects no static keys expects the instance role to be
    * used — IRSA on EKS, RRSA on ACK, an instance profile elsewhere. Hadoop
    * spells that as the absence of `access.key`, so there is no key to
    * translate; the C layer spells it `fs.use_iam`, which has to be set
    * explicitly or its provider chain is never consulted.
    */
  private def iamFallback(
      properties: Map[String, String]
  ): Map[String, String] = {
    val isLocal = properties
      .get(StorageProperties.StorageType)
      .exists(_.equalsIgnoreCase(StorageProperties.StorageTypeLocal))
    val hasStaticKeys = properties.contains(StorageProperties.AccessKeyId) &&
      properties.contains(StorageProperties.AccessKeyValue)
    val assumesRole = properties.contains(StorageProperties.RoleArn)
    val alreadySet = properties.contains(StorageProperties.UseIam)
    if (isLocal || hasStaticKeys || assumesRole || alreadySet) Map.empty
    else Map(StorageProperties.UseIam -> "true")
  }

  /** Per-bucket key first, then the global one, matching how Hadoop's own
    * connectors resolve a bucket's configuration.
    *
    * The result is what Hadoop would use for the bucket, not every key that is
    * present (review 749178e #08): the effective provider chain decides whether
    * the role keys and the static keys take part. With no provider set at
    * either level every present key is translated, as before.
    *
    * TLS (#09): Hadoop's switch becomes `fs.use_ssl`; when the deployment never
    * set it, Hadoop's default applies, which is TLS on (decided 2026-09-16),
    * unless the endpoint carries its own scheme. Path-style addressing is
    * translated only when the deployment set it: Hadoop ships a default for it,
    * and that default is not a statement about this storage. A namespace that
    * supplies no location, role or key contributes nothing, because Hadoop's
    * shipped defaults are not a storage configuration.
    *
    * A temporary credential (keys plus a session token, with a chain that uses
    * the token) cannot be translated: the native layer takes no session token
    * (storage-access.html 3.7), and the key pair alone is refused by the
    * service. When the three values are the ones this process's environment
    * holds, which is how Spark fills them, the keys are left out and the native
    * default chain reads the same variables, token included. Any other
    * temporary credential fails here, naming the options that work.
    */
  private def translate(
      conf: Configuration,
      ns: Namespace,
      bucket: String,
      environment: Map[String, String]
  ): Map[String, String] = {
    val trimmedBucket = Option(bucket).map(_.trim).getOrElse("")
    val bucketPrefix =
      if (trimmedBucket.isEmpty) None
      else Some(s"${ns.prefix}bucket.$trimmedBucket.")

    def read(key: String): Option[String] =
      Option(conf.get(key)).map(_.trim).filter(_.nonEmpty)
    def setByDeployment(key: String): Boolean =
      Option(conf.getPropertySources(key)).exists(
        _.exists(source => !source.endsWith("-default.xml"))
      )
    def lookup(suffix: String): Option[(String, String)] =
      bucketPrefix
        .map(_ + suffix)
        .filter(k => read(k).isDefined)
        .orElse(Some(ns.prefix + suffix).filter(k => read(k).isDefined))
        .map(k => k -> read(k).get)
    def flag(entry: (String, String)): Boolean = entry match {
      case (_, v) if v.equalsIgnoreCase("true")  => true
      case (_, v) if v.equalsIgnoreCase("false") => false
      case (key, v) =>
        throw new IllegalArgumentException(
          s"$key must be 'true' or 'false', got '$v'"
        )
    }
    def rows(table: Map[String, String]): Map[String, String] =
      table.iterator.flatMap { case (suffix, fsKey) =>
        lookup(suffix).map { case (_, v) => fsKey -> v }
      }.toMap

    val provider =
      effectiveProvider(conf, ns.prefix, ns.providerSuffix, trimmedBucket)
    val role =
      if (provider.forall(names(_, ns.roleProviders))) rows(ns.role)
      else Map.empty[String, String]
    val keys = {
      val present =
        if (provider.forall(names(_, ns.staticKeyProviders)))
          rows(ns.staticKeys)
        else Map.empty[String, String]
      val token = lookup(ns.sessionTokenSuffix)
      val temporary = present.nonEmpty && token.isDefined &&
        provider.forall(names(_, ns.temporaryProviders))
      if (!temporary) present
      else {
        val (tokenKey, tokenValue) = token.get
        val fromEnvironment = ns.environment.exists { case (id, secret, tok) =>
          environment
            .get(id)
            .contains(present(StorageProperties.AccessKeyId)) &&
          environment
            .get(secret)
            .contains(present(StorageProperties.AccessKeyValue)) &&
          environment.get(tok).contains(tokenValue)
        }
        if (!fromEnvironment) {
          throw new IllegalArgumentException(
            s"$tokenKey is set, so the key pair for bucket '$trimmedBucket' is a temporary " +
              "credential, and the native storage layer takes no session token. Set " +
              s"${StorageProperties.UseIam}=true to use the default credential chain " +
              "(environment variables, web identity or instance role), or pass long-term keys as " +
              s"${StorageProperties.AccessKeyId} and ${StorageProperties.AccessKeyValue}."
          )
        }
        logWarning(
          s"$tokenKey holds this process's ${ns.environment.get._3}; the key pair is not " +
            "passed to the native storage layer, which takes no session token, and its " +
            "default credential chain reads the same environment variables"
        )
        Map.empty[String, String]
      }
    }
    val supplied = rows(ns.location) ++ role ++ keys
    if (supplied.isEmpty) return Map.empty

    val endpointScheme = supplied
      .get(StorageProperties.Address)
      .map(_.toLowerCase(java.util.Locale.ROOT))
      .collect {
        case e if e.startsWith("https://") => true
        case e if e.startsWith("http://")  => false
      }
    val ssl = endpointScheme
      .orElse(lookup(ns.sslSuffix).map(flag))
      .getOrElse(true)
    val pathStyle = ns.pathStyleSuffix
      .flatMap(lookup)
      .filter { case (key, _) => setByDeployment(key) }
      .map(entry => StorageProperties.UseVirtualHost -> (!flag(entry)).toString)

    supplied ++ Map(StorageProperties.UseSSL -> ssl.toString) ++ pathStyle
  }
}
