# Spark cluster permissions and configuration

Use this guide when integrating the connector into an existing Spark cluster.
Its examples target Linux, Spark 4.0.1, Scala 2.13 and Java 21. The
[Python and Java demos](../demo/README.md) can verify the same configuration.
The connector's other Spark lines use their own
[release artifacts](releasing.md#artifact-graph).

Creating the compute environment, submitting a Spark application and reading a
Milvus snapshot require different permissions. The Spark driver and executors read
the existing snapshot directly from object storage.

## Which identity needs which permissions?

| Identity | Required access |
|---|---|
| Infrastructure operator | Provision nodes or the managed Spark service, networking, images and resource quotas. On Kubernetes, prepare the namespace, ServiceAccounts, RBAC and any Secrets or volumes. These setup permissions belong to the operator. |
| Job submitter | Submit to the chosen Spark master or YARN queue; on Kubernetes, authenticate to the API and create and monitor the driver and its supporting resources in the application namespace. |
| Spark driver | Read snapshot metadata and referenced manifests; start and monitor executors through the cluster manager; read the demo's options JSON. |
| Spark executors | Read the referenced data, delete and auxiliary files. Each executor opens its own native storage session and needs its own usable storage identity. |
| Snapshot-path helper | If the optional REST helper is used, access the Milvus endpoint and have `DescribeSnapshot` permission on the collection. Its Milvus token does not grant object-storage access. |

Driver and executor storage principals may differ, but each must be authorized
for the objects its work reads. An authenticated driver session is not shipped
to executors. Kubernetes RBAC, cloud IAM and Milvus privileges are independent.
The snapshot helper's collection-scoped privilege is defined in the
[pinned DescribeSnapshot request](../milvus-proto/proto/milvus.proto).

The connector does not require root, privileged Pods or cluster-wide
administrator rights. Its runtime OS user needs permission to read application
files, use its credentials and write its local working directories. Normal
Spark permissions to run applications still apply; configure the cluster's
[authentication and transport security](https://spark.apache.org/docs/4.0.1/security.html)
before exposing it to other users.

## Prepare every driver and executor

| Requirement | Configuration or permission |
|---|---|
| Matching runtime | Install Spark 4.0.1 / Scala 2.13 and Java 21. For Python demos, install a compatible Python interpreter and the same environment on driver and workers; set `spark.pyspark.python` to its worker path. `--py-files` does not install Python or a virtual environment. |
| Connector JARs | Distribute one assembly containing the matching native bundle, or the corresponding JVM and platform JARs. Do not combine the assembly with another copy of the connector. Make JARs available to both driver and executors through Spark's dependency distribution or the application image. |
| OS and CPU | Use a native bundle matching each JVM's architecture and a supported OS/ABI. The demo launcher selects artifacts for the submit host; use the same architecture on its workers. For Kubernetes, schedule each process on nodes matching its native bundle. |
| Native extraction | The process UID must be able to create directories, files and hard links under `java.io.tmpdir`, with enough disk space for extracted libraries. The filesystem must allow shared-library mappings; a `noexec` temporary mount can prevent JNI loading. Configure a different directory before JVM startup if needed. |
| Spark working space | Provide writable local space for shuffle, spill and downloaded dependencies. On Kubernetes with a read-only root filesystem, mount writable volumes for `/tmp` and Spark's local directories. These are local runtime writes, not snapshot writes. |
| Signal chaining | Before each JVM starts, preload that JVM's JRE library: `LD_PRELOAD=/path/to/its/jdk/lib/libjsig.so`. Configure the remote driver's environment too; exporting on the submit host only affects a client-mode driver. |
| Session integration | The public reader requires no Spark extension or plugin. |
| Heap and native memory | Size executor heap **and** container memory overhead. Arrow, Storage, Python and other native allocations are outside the JVM heap. Set `spark.executor.memoryOverhead` explicitly on Kubernetes/YARN; standalone hosts need physical RAM beyond the advertised executor heap. |

The native loader's local extraction behavior is implemented in
[NativeLibraries](../native/runtime/src/main/java/com/zilliz/milvus/jni/runtime/NativeLibraries.java).
See the [artifact and runtime contract](releasing.md#artifact-graph).
Increasing JVM heap alone does not budget native memory.

## Grant read access to the whole snapshot

Both driver and executors need the effective `fs.*` / `extfs.*` settings for
the input. Start from the [S3 options template](../demo/config/s3-options.json)
and replace the endpoint, bucket, region and snapshot path. Keep the
API-returned `milvus.snapshot.path` unchanged. `fs.root_path` does not add a
prefix to bucket-relative object keys.

Grant object reads for the snapshot JSON, snapshot Avro manifests, V3
`_metadata` manifests, column groups, deletes and auxiliary files.
They may occupy different prefixes; granting only `files/snapshots/` is
insufficient. Listing can also be needed for manifest discovery. This AWS
example covers an input whose entire referenced object set is under `files/`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:GetBucketLocation",
      "Resource": "arn:aws:s3:::your-milvus-bucket"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::your-milvus-bucket"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::your-milvus-bucket/files/*"
    }
  ]
}
```

Replace the bucket and every required prefix, including any snapshot path
outside `files/`. This baseline allows listing names throughout that bucket;
object contents remain restricted to the `GetObject` prefixes. Native metadata
existence probes can need bucket-level listing permission to distinguish a
missing object from denied access; validate tighter listing conditions against
the actual backend before using them. `GetObject` also authorizes object HEAD
requests. See [AWS HeadObject permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html).
S3 objects
encrypted with a customer-managed KMS key additionally require `kms:Decrypt`
and a compatible key policy. Cross-account bucket policies must authorize the
chosen principals too. See [AWS's operation-to-permission mapping](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html).
The connector does not need bucket creation, object upload or object deletion
permissions for these reads.

With `fs.use_iam=true`, configure the native storage credential chain separately
in **both** driver and executor processes. On EKS with
[IRSA](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html),
associate the storage role with each selected ServiceAccount and ensure
`AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` and the readable projected token
reach both driver and executor containers. Kubernetes API
permissions on that ServiceAccount do not grant S3 access. Elsewhere, use the
provider supported by your storage backend; configuring only a Java Hadoop
credential-provider class does not configure the native C++ SDK.

Use [the storage reference](reference.md#22-storage-location-and-credentials)
for static keys, AssumeRole and other clouds. Keep credential files private and
use the platform's Secret mechanism when needed. Do not paste temporary
driver credentials into static `fs.*` options: each process needs a renewable
identity. Any token file must exist and be readable where that process runs;
an `fs.ssl_ca_cert` path in shared read options must exist at that configured
path on every reader. Native storage TLS trust is separate from the JVM
truststore.

Spark staging, event logs, checkpoints or application output may need their own
write permissions in separate locations. Keep those distinct from the
connector's read-only Milvus input policy. Retain every referenced snapshot
file for the job's full duration, including retries; describing a snapshot does
not extend retention.

## Network access

| Connection | What must be reachable |
|---|---|
| Submitter to cluster manager | Standalone master, YARN ResourceManager or Kubernetes API, with the manager's authentication and CA configuration. |
| Kubernetes driver to API server | The driver Pod must reach the Kubernetes API over HTTPS to create, watch and clean up executors and supporting resources. |
| Driver and executors to storage | The configured object-storage endpoint, DNS, TLS trust and any IAM/STS endpoint or projected-token exchange needed by the chosen provider. |
| Driver and executors to each other | Spark RPC, block transfer and shuffle traffic. NetworkPolicy/firewall rules must allow the ports used by the deployment. |
| Nodes to image registry | The kubelet/container runtime must pull the image; private images need the appropriate image-pull identity/Secret. |
| Submitter and application processes to artifact services | When dependencies are not preinstalled, allow the submitter and the driver/executor processes doing downloads to reach their staging store or dependency repository. |
| REST helper to Milvus | Only the helper needs the Milvus API endpoint; Spark snapshot reads do not connect to Milvus or etcd. |

In client mode, workers must reach the submit host's advertised
`spark.driver.host`. Set `spark.driver.port` and
`spark.driver.blockManager.port` if the firewall requires fixed ports, and allow
executor block/shuffle traffic too. Opening only the master port is
insufficient. Do not use `localhost` for storage or driver addresses shared
across nodes; it refers to each node or container itself. See
[Spark networking configuration](https://spark.apache.org/docs/4.0.1/configuration.html#networking).

## Standalone or YARN with the demo launcher

For standalone, an operator installs the runtime on the master and workers,
prepares writable log/work directories for their OS user, and starts the
services. For example, run the first command on the master and the second on
each worker, with `JAVA_HOME` and `SPARK_HOME` already set:

```bash
"$SPARK_HOME/sbin/start-master.sh" --host spark-master.internal --port 7077
"$SPARK_HOME/sbin/start-worker.sh" spark://spark-master.internal:7077 \
  --cores 4 --memory 8g --work-dir /var/lib/spark/work
```

These are example resource values. Reserve additional physical RAM for native
allocations and the OS: standalone's advertised worker memory is not a native
memory limit. Using `start-workers.sh` for remote startup also requires the
operator's SSH access; individual service startup does not require SSH from
each application. See [Spark standalone deployment](https://spark.apache.org/docs/4.0.1/spark-standalone.html).

Once the cluster is available, a submitter can run either demo language:

```bash
MASTER=spark://spark-master.internal:7077 ./demo/run.sh python 01_read_snapshot.py \
  --options /private/read-options.json
MASTER=spark://spark-master.internal:7077 ./demo/run.sh java FilterProject \
  --options /private/read-options.json
```

The launcher always selects **client** deploy mode. The submit host remains the
driver, must stay running and must be reachable from workers. It sends its own
absolute `JAVA_HOME/lib/libjsig.so` path to executors; that path must exist on
every worker, or use direct `spark-submit` with each runtime's correct path.
Use custom options; the checkout's default fixture is deliberately local-only.
The options JSON needs to exist on the **driver**; local snapshot/data
paths, native CA files and credential files must exist at the appropriate paths
on every process that reads them. Java launch also needs write access to
`demo/target` on the submit host.

Additional Spark flags go in the deployment's `spark-defaults.conf` selected by
`SPARK_CONF_DIR`, or in a direct `spark-submit` command. Arguments after the
demo name are application arguments, not Spark configuration. `PYTHON` in the
launcher selects its helper interpreter; use `spark.pyspark.python` for workers.

For YARN, use `MASTER=yarn`, configure `HADOOP_CONF_DIR` or `YARN_CONF_DIR`, and
obtain the site's queue-submission and staging-directory permissions, including
Kerberos credentials where required. Configure Java 21 for Spark applications
independently of the Hadoop daemon JDK. A remote YARN driver needs its
environment through `spark.yarn.appMasterEnv.*`, while executors use
`spark.executorEnv.*`. See [Spark on YARN](https://spark.apache.org/docs/4.0.1/running-on-yarn.html).
Use Kubernetes or YARN for Python cluster deployment; Spark standalone cluster
deploy mode does not support Python applications.

## Kubernetes applications

Use the Spark runtime and application submission process supplied by your
platform. Add the matching connector artifacts with `--packages` or distribute
the JVM and native JARs together; see [installation](../README.md#installing).
The application's Spark version, Scala version and native architecture must
match the selected artifacts on the driver and executors.

Configure the remote driver's signal library before its JVM starts, as well as
the executor environment:

```text
spark.kubernetes.driverEnv.LD_PRELOAD=/path/in/driver/jdk/lib/libjsig.so
spark.executorEnv.LD_PRELOAD=/path/in/executor/jdk/lib/libjsig.so
```

Use paths that exist in the selected Spark runtime. Configure writable native
extraction and Spark working directories, and budget container memory for
native allocations in addition to the JVM heap. The driver needs Kubernetes
permissions to manage executors; each process separately needs its storage
identity, readable credential files and access to all snapshot objects.

Follow Spark's [dependency management](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html#dependency-management)
for application and JAR staging, and its [Secret management](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html#secret-management)
for mounted credentials and configuration. A local file on the submit host is
not automatically available at the same path in a remote driver or executor.
Check the driver status and application logs to confirm successful completion.

## Check the setup before running a large job

For Kubernetes, have the operator check RBAC using an identity allowed to
impersonate the ServiceAccount; the submitter can check its own identity. Set
`DRIVER_ACCOUNT` to the ServiceAccount configured for your Spark driver and
replace `spark-demo` with the application namespace:

```bash
DRIVER_ACCOUNT="your-driver-account"
kubectl auth can-i create pods -n spark-demo
kubectl auth can-i patch configmaps -n spark-demo
kubectl auth can-i create pods -n spark-demo \
  --as="system:serviceaccount:spark-demo:$DRIVER_ACCOUNT"
kubectl auth can-i deletecollection configmaps -n spark-demo \
  --as="system:serviceaccount:spark-demo:$DRIVER_ACCOUNT"
```

Then run the read example with the real options file and confirm that executor
tasks read and validate the snapshot. The [demo walkthrough](../demo/README.md)
provides these checks. A column replacement additionally needs write access to
the snapshot's segment directories and to its write result address; see
[column replacement](column-replacement.md).

| Symptom | Check first |
|---|---|
| Kubernetes `Forbidden` creating the driver | Submitter identity and namespace permissions. |
| Driver runs but cannot create/clean up executors | Driver ServiceAccount binding, Pod/ConfigMap permissions, `deletecollection`, and any PVC permissions enabled by the configuration. |
| `Pending` or `ImagePullBackOff` Pods | CPU/memory quota, node architecture, image name and registry pull credentials. |
| Driver reads metadata but executor receives storage `403` | Executor's own IAM/provider setup and permission for the referenced data, delete and index prefixes. |
| Native extraction/load failure | Writable temporary directory, hard-link support, executable shared-library mappings, matching OS/CPU bundle, JRE `libjsig` and one connector copy per JVM. |
| Tasks cannot connect to the driver or storage | Advertised addresses, DNS, CA trust, RPC/shuffle firewall rules and NetworkPolicy. |
| Container OOM with free JVM heap | Native/Python memory and executor overhead, actual segment sizes and task concurrency. |
| Options file missing in cluster mode | Its path in the driver container, Secret mount or dependency localization, not the submit host's filesystem. |

The
[read integration tests](contributing.md#tests-that-need-the-native-library)
exercise local Spark against isolated object storage; they do not establish that
your namespace RBAC, cloud IAM or multi-node network configuration has been tested.
