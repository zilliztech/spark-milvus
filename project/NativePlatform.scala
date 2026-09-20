import java.util.Locale

/** The platform a native artifact is built for, and how its files are named
  * there.
  *
  * The build cannot depend on the modules it builds, so these rules are stated
  * once here for sbt and once in `native-runtime`'s `NativeLibraries.platform`
  * and `libraryName` for the loader. The two must produce the same strings: a
  * bundle is only usable on a host whose platform string matches the one its
  * manifest declares.
  */
object NativePlatform {

  /** The host platform, spelled the way the runtime loader spells it. */
  def current: String = {
    val name = sys.props("os.name").toLowerCase(Locale.ROOT)
    val os =
      if (name.contains("linux")) "linux"
      else if (name.contains("mac") || name.contains("darwin")) "darwin"
      else if (name.contains("windows")) "windows"
      else sys.error(s"Unsupported native platform: $name")
    val arch = sys.props("os.arch").toLowerCase(Locale.ROOT) match {
      case "amd64" | "x86_64"  => "x86_64"
      case "aarch64" | "arm64" => "aarch64"
      case value => sys.error(s"Unsupported native architecture: $value")
    }
    s"$os-$arch"
  }

  def isDarwin(platform: String): Boolean = platform.startsWith("darwin-")

  def isWindows(platform: String): Boolean = platform.startsWith("windows-")

  /** The file name a shared library carries on this platform. Mach-O puts the
    * version before the suffix where ELF puts it after.
    */
  def libraryName(
      platform: String,
      base: String,
      version: Option[String] = None
  ): String =
    if (isWindows(platform)) {
      require(version.isEmpty, s"A Windows library carries no version: $base")
      s"$base.dll"
    } else if (isDarwin(platform))
      "lib" + base + version.map("." + _).getOrElse("") + ".dylib"
    else "lib" + base + ".so" + version.map("." + _).getOrElse("")

  /** Matches one shared library file name on this platform, versioned or not.
    */
  def libraryPattern(
      platform: String,
      stem: String = "[A-Za-z0-9_+.-]+"
  ): String =
    if (isWindows(platform)) stem + "\\.dll"
    else if (isDarwin(platform)) stem + "(?:\\.[0-9][^.]*)*\\.dylib"
    else stem + "\\.so(?:\\..*)?"

  /** The system zlib the JVM has already loaded before JNI initialization; a
    * second copy in the bundle would leave the process's provider undecided.
    */
  def systemZlib(platform: String): String =
    if (isWindows(platform)) "zlib1.dll"
    else if (isDarwin(platform)) "libz.1.dylib"
    else "libz.so.1"

  /** The two JNI libraries a JVM loads directly. */
  def jvmLoadEntries(platform: String): Vector[String] = Vector(
    libraryName(platform, "milvus-storage-jni"),
    libraryName(platform, "knowhere_jni")
  )

  /** Every library the audit opens directly, JNI entries first. */
  def auditDlopenEntries(platform: String): Vector[String] = Vector(
    libraryName(platform, "milvus-storage-jni"),
    libraryName(platform, "milvus-storage"),
    libraryName(platform, "knowhere_jni"),
    libraryName(platform, "knowhere_c", Some("1")),
    libraryName(platform, "knowhere")
  )

  /** The storage engine and its JNI library, which every bundle carries. */
  def storageEntries(platform: String): Vector[String] = Vector(
    libraryName(platform, "milvus-storage"),
    libraryName(platform, "milvus-storage-jni")
  )

  /** The Cardinal plugins, present only in a Cardinal-enabled build. */
  def cardinalPlugins(platform: String): Set[String] = Set(
    libraryName(platform, "cardinalv1"),
    libraryName(platform, "cardinalv2")
  )
}
