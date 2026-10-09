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

  /** What a Hadoop credential provider class takes its identity from, in the
    * terms the native layer has: static keys, its default credential chain, or
    * a role assumed through STS.
    */
  private sealed trait Source
  private object Source {

    /** The static keys; a session token is ignored. */
    case object Keys extends Source

    /** The static keys with a session token; skipped without one. */
    case object KeysWithToken extends Source

    /** The static keys, with the session token when one is set. */
    case object KeysWithOptionalToken extends Source

    /** A role assumed through STS. */
    case object Role extends Source

    /** The environment's own identity: environment variables, a web identity
      * token, a profile, a container or instance role. The native default
      * credential chain resolves the same sources.
      */
    case object Platform extends Source
  }

  /** The identity Hadoop uses for one bucket, as the native layer takes it. */
  private sealed trait Identity
  private object Identity {

    /** No provider chain is configured (a Configuration without Hadoop's
      * shipped defaults): every present key is translated.
      */
    case object Unconfigured extends Identity
    final case class StaticKeys(withToken: Boolean) extends Identity
    case object AssumedRole extends Identity
    case object Environment extends Identity
  }

  /** One Hadoop namespace: the key suffixes that make up a storage
    * configuration and the provider classes that decide which of them carry the
    * identity.
    *
    * `sessionTokenSuffix` names the token of a temporary credential.
    * `environment` names the variables (key id, secret, token) the platform's
    * default credential chain reads, when Spark copies them into this
    * namespace. `singleProvider` is true where Hadoop takes one provider class,
    * not a list. `roleSigner` is the key of the chain that signs the AssumeRole
    * call, with Hadoop's built-in value when the key is absent; None where no
    * such key exists.
    */
  private final case class Namespace(
      prefix: String,
      providerSuffix: String,
      location: Map[String, String],
      role: Map[String, String],
      staticKeys: Map[String, String],
      sessionTokenSuffix: String,
      environment: Option[(String, String, String)],
      sslSuffix: String,
      pathStyleSuffix: Option[String],
      sources: Map[String, Source],
      singleProvider: Boolean,
      roleSigner: Option[(String, Seq[String])]
  )

  /** The class names Hadoop's S3A and Aliyun OSS connectors assume a role with.
    */
  private[connector] val S3AAssumedRoleProvider =
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
  private[connector] val OssAssumedRoleProviders = Set(
    "com.zilliz.cloud.hadoop.AliyunOSSRoleCredentialsProvider",
    "org.apache.hadoop.fs.aliyun.oss.AssumedRoleCredentialProvider"
  )

  private val S3ASimple =
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  private val AwsSdkV2 = "software.amazon.awssdk.auth.credentials."

  /** The AWS SDK v1 and v2 providers that read the environment's identity. */
  private val AwsPlatformProviders: Set[String] = Set(
    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    "com.amazonaws.auth.ContainerCredentialsProvider",
    "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper",
    "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "com.amazonaws.auth.profile.ProfileCredentialsProvider"
  ) ++ Seq(
    "EnvironmentVariableCredentialsProvider",
    "InstanceProfileCredentialsProvider",
    "ContainerCredentialsProvider",
    "WebIdentityTokenFileCredentialsProvider",
    "DefaultCredentialsProvider",
    "ProfileCredentialsProvider"
  ).map(AwsSdkV2 + _)

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
    // Spark copies these from the driver's environment into access.key,
    // secret.key and session.token (SparkHadoopUtil).
    environment = Some(
      ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    ),
    sslSuffix = "connection.ssl.enabled",
    pathStyleSuffix = Some("path.style.access"),
    sources = Map[String, Source](
      S3ASimple -> Source.Keys,
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" ->
        Source.KeysWithToken,
      S3AAssumedRoleProvider -> Source.Role
    ) ++ AwsPlatformProviders.map(_ -> Source.Platform),
    singleProvider = false,
    // AssumedRoleCredentialProvider signs STS with this chain; without the
    // key it uses Simple, then the environment variables.
    roleSigner = Some(
      "assumed.role.credentials.provider" ->
        Seq(S3ASimple, AwsSdkV2 + "EnvironmentVariableCredentialsProvider")
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
    environment = None,
    sslSuffix = "connection.secure.enabled",
    pathStyleSuffix = None,
    sources = Map[String, Source](
      // Hadoop's Aliyun provider uses the token whenever it is set.
      "org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider" ->
        Source.KeysWithOptionalToken
    ) ++ OssAssumedRoleProviders.map(_ -> Source.Role),
    // hadoop-aliyun instantiates the value as one class name
    // (AliyunOSSUtils.getCredentialsProvider).
    singleProvider = true,
    roleSigner = None
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

  /** Whether a chain is an assumed role and nothing else: the one shape whose
    * identity is the role, which `fs.use_iam=true` keeps as the platform's data
    * role.
    */
  private[connector] def onlyAssumedRole(chain: String): Boolean = {
    val listed = classes(chain)
    listed.nonEmpty &&
    listed.forall(
      (Set(S3AAssumedRoleProvider) ++ OssAssumedRoleProviders).contains
    )
  }

  /** Whether the S3A identity Hadoop uses for `bucket` is an assumed role.
    * Backfill decides its native writer's role with this, so it and the
    * connector read one rule; a chain the native layer cannot express throws.
    */
  private[connector] def s3aAssumesRole(
      conf: Configuration,
      bucket: String,
      environment: Map[String, String] = sys.env
  ): Boolean =
    identity(new Scope(conf, s3a, bucket, environment)) == Identity.AssumedRole

  /** Whether the Aliyun OSS role keys for `bucket` are taken: the effective
    * provider assumes a role, or no provider is configured, which is how the
    * translation treats them too. A chain the native layer cannot express
    * throws.
    */
  private[connector] def ossRoleTaken(
      conf: Configuration,
      bucket: String,
      environment: Map[String, String] = sys.env
  ): Boolean =
    identity(new Scope(conf, oss, bucket, environment)) match {
      case Identity.AssumedRole | Identity.Unconfigured => true
      case _                                            => false
    }

  /** The keys of one namespace as Hadoop reads them for one bucket: the
    * per-bucket key first, then the global one.
    */
  private final class Scope(
      val conf: Configuration,
      val ns: Namespace,
      bucketName: String,
      environment: Map[String, String]
  ) {
    val bucket: String = Option(bucketName).map(_.trim).getOrElse("")
    private val bucketPrefix =
      if (bucket.isEmpty) None else Some(s"${ns.prefix}bucket.$bucket.")

    def read(key: String): Option[String] =
      Option(conf.get(key)).map(_.trim).filter(_.nonEmpty)

    def lookup(suffix: String): Option[(String, String)] =
      bucketPrefix
        .map(_ + suffix)
        .filter(k => read(k).isDefined)
        .orElse(Some(ns.prefix + suffix).filter(k => read(k).isDefined))
        .map(k => k -> read(k).get)

    def rows(table: Map[String, String]): Map[String, String] =
      table.iterator.flatMap { case (suffix, fsKey) =>
        lookup(suffix).map { case (_, v) => fsKey -> v }
      }.toMap

    def hasKeys: Boolean = ns.staticKeys.keys.forall(lookup(_).isDefined)

    /** Whether the static keys, and the session token when one is set, are the
      * values this process's environment holds, which is how Spark fills them.
      * The native default credential chain reads the same variables, so such
      * keys reach the native layer through `fs.use_iam` unchanged.
      */
    def keysFromEnvironment: Boolean =
      hasKeys && ns.environment.exists { case (id, secret, token) =>
        val keys = rows(ns.staticKeys)
        environment
          .get(id)
          .contains(keys(StorageProperties.AccessKeyId)) &&
        environment
          .get(secret)
          .contains(keys(StorageProperties.AccessKeyValue)) &&
        lookup(ns.sessionTokenSuffix).forall { case (_, value) =>
          environment.get(token).contains(value)
        }
      }

    def refuse(reason: String): Nothing =
      throw new IllegalArgumentException(
        s"$reason. The native storage layer takes one identity per bucket: static keys " +
          s"(${StorageProperties.AccessKeyId} and ${StorageProperties.AccessKeyValue}), " +
          s"its default credential chain (${StorageProperties.UseIam}=true), or a role " +
          s"assumed with that chain (${StorageProperties.RoleArn}); set those options " +
          s"for bucket '$bucket' instead."
      )
  }

  private def classes(chain: String): Seq[String] =
    chain.split(',').map(_.trim).filter(_.nonEmpty).toSeq

  /** The identity Hadoop uses for the scope's bucket, when the native layer can
    * take the same one.
    *
    * Hadoop tries the chain in order and uses the first provider that yields
    * credentials. Whether a key provider yields them follows from the
    * configuration; whether an environment provider or an assumed role does
    * depends on the machine and on STS. So the chain is walked in order: static
    * keys that are set decide where they stand; the environment's identity
    * decides when nothing behind it could apply; a role decides only as the
    * last source. Anything else is refused rather than guessed, as are a role
    * whose AssumeRole call Hadoop signs with the static keys (the native layer
    * signs it with its default chain), a class the connector cannot map, and a
    * list where Hadoop takes one class.
    */
  private def identity(scope: Scope): Identity = {
    val ns = scope.ns
    val providerKey = ns.prefix + ns.providerSuffix
    effectiveProvider(scope.conf, ns.prefix, ns.providerSuffix, scope.bucket)
      .map(classes) match {
      case None =>
        if (scope.hasKeys && scope.lookup("assumed.role.arn").isDefined) {
          scope.refuse(
            s"No $providerKey is set, and both a role and static keys are"
          )
        }
        Identity.Unconfigured
      case Some(chain) =>
        if (ns.singleProvider && chain.size > 1) {
          scope.refuse(
            s"$providerKey lists ${chain.size} classes (${chain.mkString(",")}), and Hadoop takes one"
          )
        }
        walk(scope, chain, providerKey, roleAllowed = true) match {
          case Identity.AssumedRole =>
            if (scope.hasKeys) {
              ns.roleSigner match {
                case None =>
                  scope.refuse(
                    s"$providerKey assumes a role and static keys are set, which Hadoop may sign " +
                      "the AssumeRole call with"
                  )
                case Some((signerSuffix, builtIn)) =>
                  val (signerKey, signer) = scope
                    .lookup(signerSuffix)
                    .map { case (k, v) => k -> classes(v) }
                    .getOrElse((ns.prefix + signerSuffix) -> builtIn)
                  walk(scope, signer, signerKey, roleAllowed = false) match {
                    case Identity.StaticKeys(_) if !scope.keysFromEnvironment =>
                      scope.refuse(
                        s"$signerKey (${signer.mkString(",")}) signs the AssumeRole call with " +
                          "the static keys, which the native layer cannot do"
                      )
                    case _ => ()
                  }
              }
            }
            Identity.AssumedRole
          case other => other
        }
    }
  }

  /** The source a chain resolves to, walked in Hadoop's order. */
  private def walk(
      scope: Scope,
      chain: Seq[String],
      key: String,
      roleAllowed: Boolean
  ): Identity = {
    val sources = chain.map { name =>
      scope.ns.sources.getOrElse(
        name,
        scope.refuse(
          s"$key names $name, which the connector cannot map to a native identity"
        )
      )
    }
    val keys = scope.hasKeys
    val token = scope.lookup(scope.ns.sessionTokenSuffix).isDefined
    def applies(source: Source): Boolean = source match {
      case Source.Keys                  => keys
      case Source.KeysWithToken         => keys && token
      case Source.KeysWithOptionalToken => keys
      case _                            => false
    }
    val listed = chain.mkString(",")
    sources.zipWithIndex
      .collectFirst {
        case (Source.Platform, i) =>
          if (sources.drop(i + 1).exists(s => applies(s) || s == Source.Role)) {
            scope.refuse(
              s"$key ($listed) tries the environment's identity before another source that " +
                "could apply, and which one Hadoop uses depends on the machine"
            )
          }
          Identity.Environment
        case (Source.Role, i) =>
          if (!roleAllowed) {
            scope.refuse(
              s"$key ($listed) names an assumed role, which cannot sign an AssumeRole call"
            )
          }
          if (sources.drop(i + 1).exists(_ != Source.Role)) {
            scope.refuse(
              s"$key ($listed) mixes an assumed role with another identity source behind it, " +
                "and whether AssumeRole succeeds decides which one Hadoop uses"
            )
          }
          Identity.AssumedRole
        case (source, _) if applies(source) =>
          Identity.StaticKeys(withToken = source match {
            case Source.Keys => false
            case _           => token
          })
      }
      .getOrElse(Identity.Environment)
  }

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
      environment: Map[String, String] = sys.env,
      declared: scala.collection.Map[String, String] = Map.empty
  ): Map[String, String] = {
    // Explicit fs.* options that name the identity win over the Hadoop chain,
    // which then decides nothing and is not judged.
    val declaredIdentity =
      (declared.contains(StorageProperties.AccessKeyId) &&
        declared.contains(StorageProperties.AccessKeyValue)) ||
        declared.contains(StorageProperties.RoleArn)
    val declaredCloud = declared
      .get(StorageProperties.CloudProvider)
      .map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .filter(_.nonEmpty)
    val s3a = translate(
      new Scope(conf, this.s3a, bucket, environment),
      declaredIdentity,
      forBucket = declaredCloud.forall(_ == "aws")
    )
    val oss = translate(
      new Scope(conf, this.oss, bucket, environment),
      declaredIdentity,
      forBucket = declaredCloud.contains("aliyun")
    )

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
    * present: the effective provider chain decides whether the role keys or the
    * static keys take part, and a chain the native layer cannot express is
    * refused (see [[identity]]). With no provider set at either level every
    * present key is translated, except a role together with static keys, which
    * is refused.
    *
    * TLS: Hadoop's switch becomes `fs.use_ssl`; when the deployment never set
    * it, Hadoop's default applies, which is TLS on (decided 2026-09-16), unless
    * the endpoint carries its own scheme. Path-style addressing is translated
    * only when the deployment set it: Hadoop ships a default for it, and that
    * default is not a statement about this storage. A namespace that supplies
    * no location, role or key contributes nothing, because Hadoop's shipped
    * defaults are not a storage configuration.
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
      scope: Scope,
      declaredIdentity: Boolean,
      forBucket: Boolean
  ): Map[String, String] = {
    val conf = scope.conf
    val ns = scope.ns
    val trimmedBucket = scope.bucket
    def setByDeployment(key: String): Boolean =
      Option(conf.getPropertySources(key)).exists(
        _.exists(source => !source.endsWith("-default.xml"))
      )
    def lookup(suffix: String): Option[(String, String)] = scope.lookup(suffix)
    def flag(entry: (String, String)): Boolean = entry match {
      case (_, v) if v.equalsIgnoreCase("true")  => true
      case (_, v) if v.equalsIgnoreCase("false") => false
      case (key, v) =>
        throw new IllegalArgumentException(
          s"$key must be 'true' or 'false', got '$v'"
        )
    }
    def rows(table: Map[String, String]): Map[String, String] =
      scope.rows(table)

    // A namespace that supplies nothing for a bucket of another cloud (the
    // fs.oss.* of a session reading an S3 bucket) is not this bucket's
    // configuration, and its chain is not judged.
    val supplies = rows(ns.location) ++ rows(ns.role) ++ rows(ns.staticKeys)
    if (supplies.isEmpty && !forBucket) return Map.empty

    val chosen =
      if (declaredIdentity) Identity.Environment else identity(scope)
    val role = chosen match {
      case Identity.Unconfigured | Identity.AssumedRole => rows(ns.role)
      case _ => Map.empty[String, String]
    }
    val keys = {
      val (present, temporary) = chosen match {
        case Identity.Unconfigured =>
          val keys = rows(ns.staticKeys)
          (keys, keys.nonEmpty && lookup(ns.sessionTokenSuffix).isDefined)
        case Identity.StaticKeys(withToken) =>
          (rows(ns.staticKeys), withToken)
        case _ => (Map.empty[String, String], false)
      }
      if (!temporary) present
      else {
        val (tokenKey, _) = lookup(ns.sessionTokenSuffix).get
        if (!scope.keysFromEnvironment) {
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
