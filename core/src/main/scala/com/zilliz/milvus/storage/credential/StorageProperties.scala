package com.zilliz.milvus.storage.credential

/** Builds the `fs.*` property map the native layer takes.
  *
  * This is the only place that parses storage configuration. It validates what
  * the C layer needs and hands the rest through unchanged: the C property
  * registry checks types on read, so listing every known key here would only
  * mean editing Scala whenever milvus-storage adds one.
  *
  * No cloud name and no scheme appear below. Which backend runs is
  * `fs.cloud_provider`, resolved in the C layer.
  */
object StorageProperties {

  val Prefix = "fs."
  val ExternalPrefix = "extfs."

  val StorageType = "fs.storage_type"
  val CloudProvider = "fs.cloud_provider"
  val Address = "fs.address"
  val BucketName = "fs.bucket_name"
  val RootPath = "fs.root_path"
  val Region = "fs.region"
  val UseSSL = "fs.use_ssl"
  val UseIam = "fs.use_iam"
  val AccessKeyId = "fs.access_key_id"
  val AccessKeyValue = "fs.access_key_value"
  val RoleArn = "fs.role_arn"
  val SessionName = "fs.session_name"
  val ExternalId = "fs.external_id"

  /** Value of `fs.storage_type` that reads the local filesystem, where bucket,
    * endpoint and credentials do not apply.
    */
  val StorageTypeLocal = "local"
  private val StorageTypeRemote = "remote"

  /** Conventions, not developer conveniences: `files` is where Milvus puts
    * segment data, and these two match the C layer's own defaults.
    */
  private val Defaults = Map(
    StorageType -> StorageTypeRemote,
    RootPath -> "files",
    UseSSL -> "false",
    // Inherited from the code this replaces. A region is as arbitrary as a
    // bucket name, but dropping it changes the SigV4 signing region, so it
    // stays until someone decides that deliberately.
    Region -> "us-west-2"
  )

  /** Location and credentials carry no defaults.
    *
    * A missing bucket or endpoint used to fall back to the MinIO values a
    * developer runs locally, so a deployment that forgot one option connected
    * to the wrong place and failed at request time. Decision 21 settled this:
    * missing means fail at startup.
    */
  def from(
      options: scala.collection.Map[String, String]
  ): Map[String, String] = {
    val cleaned = clean(options)
    val default = cleaned.filter { case (k, _) => k.startsWith(Prefix) }
    val out = Map.newBuilder[String, String]
    out ++= validated(Defaults ++ default, "")

    externalNames(cleaned).foreach { name =>
      val prefix = s"$ExternalPrefix$name."
      val group = cleaned.collect {
        case (k, v) if k.startsWith(prefix) =>
          (Prefix + k.substring(prefix.length)) -> v
      }
      validated(Defaults ++ group, s"$ExternalPrefix$name.").foreach {
        case (k, v) => out += (prefix + k.substring(Prefix.length)) -> v
      }
    }
    out.result()
  }

  /** The names registered under `extfs.<name>.*`, in first-seen order. */
  def externalNames(
      options: scala.collection.Map[String, String]
  ): Seq[String] =
    options.keys.toSeq
      .filter(_.startsWith(ExternalPrefix))
      .flatMap { key =>
        val rest = key.substring(ExternalPrefix.length)
        val dot = rest.indexOf('.')
        if (dot > 0) Some(rest.substring(0, dot)) else None
      }
      .distinct
      .sorted

  private def clean(
      options: scala.collection.Map[String, String]
  ): Map[String, String] =
    options.iterator.collect {
      case (k, v)
          if k != null &&
            (k.startsWith(Prefix) || k.startsWith(ExternalPrefix)) &&
            v != null && v.trim.nonEmpty =>
        k -> v.trim
    }.toMap

  private def validated(
      group: Map[String, String],
      label: String
  ): Map[String, String] = {
    val isRemote = !group(StorageType).equalsIgnoreCase(StorageTypeLocal)
    if (!isRemote) return group - AccessKeyId - AccessKeyValue

    require(group, BucketName, label)
    require(group, Address, label)

    // Under IAM or AssumeRole the native layer resolves credentials itself, and
    // sending empty keys would override whatever it finds. Otherwise both keys
    // are needed: one without the other signs requests the backend rejects.
    val assumesRole = group.contains(RoleArn)
    val usesIam = group.get(UseIam).exists(_.equalsIgnoreCase("true"))
    if (assumesRole || usesIam) {
      group - AccessKeyId - AccessKeyValue
    } else {
      require(group, AccessKeyId, label)
      require(group, AccessKeyValue, label)
      group
    }
  }

  private def require(
      group: Map[String, String],
      key: String,
      label: String
  ): Unit =
    if (!group.contains(key)) {
      val name =
        if (label.isEmpty) key else label + key.substring(Prefix.length)
      throw new IllegalArgumentException(s"$name must be set")
    }
}
