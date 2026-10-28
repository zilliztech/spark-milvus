# Spark cluster permissions and configuration

Use this guide when moving the [Python and Java demos](../demo/README.md) from
`local[2]` to a Spark cluster. Its concrete commands target Linux, Spark 4.0.1,
Scala 2.13 and Java 21, matching the demo launcher. The connector's other Spark
lines use their own
[release artifacts](releasing.md#maven-coordinates-and-runtime-requirements).

Creating the compute environment, submitting a Spark application and reading a
Milvus snapshot require different permissions. The Spark driver and executors read
the existing snapshot directly from object storage.

## Which identity needs which permissions?

| Identity | Required access |
|---|---|
| Infrastructure operator | Provision nodes or the managed Spark service, networking, images and resource quotas. On Kubernetes, prepare the namespace, ServiceAccounts, RBAC and any Secrets or volumes. These setup permissions belong to the operator. |
| Job submitter | Submit to the chosen Spark master or YARN queue; on Kubernetes, authenticate to the API and create and monitor the driver and its supporting resources in the application namespace. |
| Spark driver | Read snapshot metadata and referenced manifests; start and monitor executors through the cluster manager; read the demo's options and query JSON. |
| Spark executors | Read the referenced data, delete, index and auxiliary files. Each executor opens its own native storage session and needs its own usable storage identity. |
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
| OS and CPU | Use a native bundle matching each JVM's architecture and a supported OS/ABI. The demo launcher selects artifacts for the submit host; use the same architecture on its workers. For the Kubernetes example below, select `amd64` nodes and an x86_64 image/bundle; change both for ARM64. |
| Native extraction | The process UID must be able to create directories, files and hard links under `java.io.tmpdir`, with enough disk space for extracted libraries. The filesystem must allow shared-library mappings; a `noexec` temporary mount can prevent JNI loading. Configure a different directory before JVM startup if needed. |
| Spark working space | Provide writable local space for shuffle, spill and downloaded dependencies. On Kubernetes with a read-only root filesystem, mount writable volumes for `/tmp` and Spark's local directories. These are local runtime writes, not snapshot writes. |
| Signal chaining | Before each JVM starts, preload that JVM's JRE library: `LD_PRELOAD=/path/to/its/jdk/lib/libjsig.so`. Configure the remote driver's environment too; exporting on the submit host only affects a client-mode driver. |
| Session integration | Set `spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions` and `spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin` before creating Spark. Preserve any other extensions/plugins the deployment requires. |
| Heap and native memory | Size executor heap **and** container memory overhead. Arrow, Knowhere, Python and other native allocations are outside the JVM heap. Set `spark.executor.memoryOverhead` explicitly on Kubernetes/YARN; standalone hosts need physical RAM beyond the advertised executor heap. |

The native loader's local extraction behavior is implemented in
[NativeLibraries](../native/runtime/src/main/java/com/zilliz/milvus/jni/runtime/NativeLibraries.java).
See the [runtime launch requirements](releasing.md#thin-modules-for-downstream-libraries)
and [search resource configuration](reference.md#vector-search).

For persisted-index search from object storage on glibc systems, include the
three executor `MALLOC_*` settings in the Kubernetes example below; the
[search reference](reference.md#vector-search) explains their purpose.
Keep dynamic allocation disabled for the connector's automatic APPROX task
resource assignment. If enabling it, follow the reference's `spark.task.cpus`
requirements. Increasing `-Xmx` alone does not budget native memory.

## Grant read access to the whole snapshot

Both driver and executors need the effective `fs.*` / `extfs.*` settings for
the input. Start from the [S3 options template](../demo/config/s3-options.json)
and replace the endpoint, bucket, region and snapshot path. Keep the
API-returned `milvus.snapshot.path` unchanged. `fs.root_path` does not add a
prefix to bucket-relative object keys.

Grant object reads for the snapshot JSON, snapshot Avro manifests, V3
`_metadata` manifests, column groups, deletes, indexes and auxiliary files.
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
MASTER=spark://spark-master.internal:7077 ./demo/run.sh java ExactSearch \
  --options /private/read-options.json --queries /private/queries.json
```

The launcher always selects **client** deploy mode. The submit host remains the
driver, must stay running and must be reachable from workers. It sends its own
absolute `JAVA_HOME/lib/libjsig.so` path to executors; that path must exist on
every worker, or use direct `spark-submit` with each runtime's correct path.
Use custom options; the checkout's default fixture is deliberately local-only.
The options and query JSON need to exist on the **driver**; local snapshot/data
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

## Build the application image

The [connector image](../docker/README.md) packages Spark 4.0.1, Scala 2.13,
Java 21, Python, the connector assembly and both languages' example programs.
Build the matching native bundle and `spark/assembly` first, following the
[demo runtime instructions](../demo/README.md#prepare-the-runtime). Then package
that assembly; replace the JAR and registry below with your own:

```bash
export CONNECTOR_JAR="/absolute/path/to/spark-milvus-4.0-bundle-<version>.jar"
./docker/build.sh \
  --assembly "$CONNECTOR_JAR" \
  --image registry.example.com/spark-milvus-demo:4.0.1-jdk21-amd64 \
  --platform linux/amd64
```

The build helper validates the assembly's native platform and stages only
explicit public demo files. Options, credentials and the rest of the checkout
are excluded. It compiles all Java entry points into `demo-java.jar`; cluster
submission does not compile source. The image stays local unless `--push` is
supplied. Registry publishing needs the build operator's credentials; registry
pull access belongs to the Kubernetes nodes or configured image-pull Secrets.
For repeatable deployment, pin the base image using `--base-image` and set the
resulting application image digest in Helm values.

The runtime uses non-root UID/GID `185:185`, supplies the native signal-chaining
and allocator environment, and preserves Spark's `/opt/entrypoint.sh` for its
driver and executor modes. Its writable working directory is
`/opt/spark/work-dir`, separate from the immutable JARs under
`/opt/spark-milvus`. This distinction also prevents dependency localization
from overwriting an image-baked source JAR. See the image README for the full
filesystem contract and local smoke commands.

## Kubernetes deployment templates

Use the [Helm chart](../deploy/helm/spark-milvus-demo/README.md). It uses native
Spark on Kubernetes and needs no Spark Operator. Provision the Kubernetes
cluster and node capacity first; Spark does not need a separate master/worker
Deployment in this mode.

The chart creates a submission Job, pod-template ConfigMap, three
ServiceAccounts and namespace-scoped RBAC. The Job runs the image's
`spark-submit` against the in-cluster Kubernetes API in **cluster** deploy mode.
Spark creates the driver Pod and supporting resources; the driver creates
executor Pods. The chart defaults to the vector-functions example, which
requires no snapshot or storage credentials. Select a snapshot example and an
existing input Secret to read real data.

### Operator permissions and runtime accounts

The chart installer needs permissions to manage Jobs, ConfigMaps,
ServiceAccounts, Roles and RoleBindings in the application namespace, plus the
release-storage Secrets used by Helm's default storage backend. Kubernetes also
checks whether the installer can grant the Role's permissions. The operator
creating input or registry Secrets needs the corresponding Secret permission.
Creating a namespace with `--create-namespace` requires namespace creation
permission; the chart itself does not own or delete namespaces.

A platform operator can instead provision the accounts and bindings, then set
`serviceAccounts.<role>.create=false`, the account's `name`, and
`rbac.create=false`. These settings support pre-existing workload-identity
accounts. They do not grant the installer additional privileges.

The runtime accounts have separate responsibilities:

| Account | Kubernetes API access in this template |
|---|---|
| Submitter | Create, monitor and clean up driver Pods and their supporting Services/ConfigMaps. Its API token and CA are mounted in the submission Job. |
| Driver | Create, monitor and clean up executor Pods and supporting Services/ConfigMaps. Its account is explicitly independent of the submitter. |
| Executor | No application Kubernetes API Role. Configure its object-storage identity independently. |

The baseline Role allows `create/get/list/watch/patch/delete/deletecollection`
on Pods, Services and ConfigMaps in one namespace. `patch` is needed for
[server-side apply](https://kubernetes.io/docs/reference/using-api/server-side-apply/#access-control-and-permissions)
and Pod lifecycle updates; collection deletion supports Spark's label-filtered
cleanup. It grants no Secret reads or cluster-scoped access. Spark's
[Kubernetes RBAC documentation](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html#rbac)
explains the account model. Additional schedulers, PVCs or custom feature steps
need their own permission review.

The chart explicitly disables driver PVC ownership/reuse and dynamic
allocation. Its submitter, driver and executor pod templates use non-root UID
185, drop capabilities, disable privilege escalation, set RuntimeDefault
seccomp and use read-only root filesystems with writable `/tmp` and
`/opt/spark/work-dir` volumes. Kubernetes admission policy must permit these
Pods and Spark's added local-storage volumes.

### Provide the input and values

Prepare `read-options.json` using the API-returned snapshot location and the
storage configuration above. Prepare query vectors matching your snapshot's
dimension. After the operator creates the namespace, create a namespace-local
Secret containing both JSON files:

```bash
kubectl -n spark-demo create secret generic milvus-read-options \
  --from-file=read-options.json=/private/read-options.json \
  --from-file=queries.json=/private/queries.json
```

The chart references this existing Secret. It does not put its contents in Helm
values, create it, or require the application's ServiceAccounts to read it
through the API. Spark mounts it once in the driver at
`/etc/spark-milvus/input`; options and queries are read there before creating
Spark. Secret volume mounting is performed by Kubernetes, separately from
application RBAC. Use a DNS label of at most 56 characters for its name, as
Spark appends a suffix when naming the volume.

The options Secret does not establish a cloud identity. For IAM-based storage,
configure **both** driver and executor accounts and any required annotations.
Provider token files or custom CA files needed by executors must be available
there too; use the platform's identity injection or Spark Secret mounts through
`extraSparkConf`. See [Spark Secret management](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html#secret-management).
Additional Spark configuration is stored in the Helm release and submission
arguments; keep credentials in workload identity or existing Secret files.

Save the following as `/private/spark-demo-values.yaml`:

```yaml
image:
  repository: registry.example.com/spark-milvus-demo
  tag: "4.0.1-jdk21-amd64"
  # Prefer digest: sha256:... after publishing the image.
language: python
example: exact
input:
  existingSecret: milvus-read-options
  optionsKey: read-options.json
  queriesKey: queries.json
nodeSelector:
  kubernetes.io/os: linux
  kubernetes.io/arch: amd64
```

For Java, set `language: java`; `example: exact` then selects the precompiled
`ExactSearch` class. The chart also supports `read`, `filter`, `sql`,
`filtered`, `index`, `vectors` and `partition`. Use `image.pullSecrets` for a
private registry and adjust both image platform and node selector for ARM64.
See the chart's [values](../deploy/helm/spark-milvus-demo/values.yaml) for the
complete configuration, including existing accounts and resource sizing.

The defaults request two executors with 4 GiB heap and 4 GiB overhead each,
plus a driver with 2 GiB heap and 1 GiB overhead and a separate submission Job.
These are example budgets, not production sizing. Ensure quotas and nodes can
satisfy them, then tune for your actual indexes, segment sizes and Python/native
memory use. Default ServiceAccount names are scoped to each release; fixed
account names should be provisioned separately and selected with `create=false`
when sharing them across releases.

### Render, install and check the application

Render the deployment templates for review, then install with the operator's
configured Kubernetes identity:

```bash
helm lint deploy/helm/spark-milvus-demo \
  --values /private/spark-demo-values.yaml
helm template milvus-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo --values /private/spark-demo-values.yaml \
  > /private/spark-demo-rendered.yaml
helm install milvus-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo --values /private/spark-demo-values.yaml
```

This is a one-shot submission: use a fresh release name for a new run and follow
the chart README's cleanup steps before reusing a name. The chart sets
`spark.kubernetes.submission.waitAppCompletion=true`, but Spark 4.0.1 can return
success from the submission process after a **failed** driver becomes terminal.
A completed submission Job or `helm --wait-for-jobs` alone is therefore not an
application-success check. Inspect the release's driver phase and logs:

```bash
kubectl -n spark-demo get pods \
  -l app.kubernetes.io/instance=milvus-demo,spark-role=driver \
  -o custom-columns=NAME:.metadata.name,PHASE:.status.phase
kubectl -n spark-demo logs \
  -l app.kubernetes.io/instance=milvus-demo,spark-role=driver \
  --all-containers=true
```

Require driver phase `Succeeded` and the example's `Demo passed` output. The
monitoring identity needs `get/list` on Pods and `get` on `pods/log`; the runtime
Role does not include log-reading permissions.

All application resources use image-local `local:///` paths. The connector is
read-only and needs no Spark artifact-upload credentials for this chart.
Deployments using external application files or staging storage must configure
Spark's upload mechanism and its Hadoop-compatible storage credentials
separately. `--files` does not rewrite an absolute application argument from a
submit-host path into a driver path. See [Kubernetes dependency management](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html#dependency-management)
and [application submission](https://spark.apache.org/docs/4.0.1/submitting-applications.html).

## Check the setup before running a large job

For Kubernetes, have the operator check RBAC using an identity allowed to
impersonate the ServiceAccount; the submitter can check its own identity. Set
`DRIVER_ACCOUNT` to the driver account name in the rendered templates, or the
existing account selected in values:

```bash
DRIVER_ACCOUNT="your-rendered-driver-account"
kubectl auth can-i create pods -n spark-demo
kubectl auth can-i patch configmaps -n spark-demo
kubectl auth can-i create pods -n spark-demo \
  --as="system:serviceaccount:spark-demo:$DRIVER_ACCOUNT"
kubectl auth can-i deletecollection configmaps -n spark-demo \
  --as="system:serviceaccount:spark-demo:$DRIVER_ACCOUNT"
```

Then run the read example with the real options file and confirm that executor
tasks can read the snapshot, not just that the driver starts. Run exact and
indexed search and check their native metrics; merely starting a Spark session
or returning search results does not prove that native indexes were used.
The [demo walkthrough](../demo/README.md) and
[disposable Milvus procedure](../demo/live-validation.md) provide those checks.

| Symptom | Check first |
|---|---|
| Kubernetes `Forbidden` creating the driver | Submitter identity and namespace permissions. |
| Driver runs but cannot create/clean up executors | Driver ServiceAccount binding, Pod/ConfigMap permissions, `deletecollection`, and any PVC permissions enabled by the configuration. |
| `Pending` or `ImagePullBackOff` Pods | CPU/memory quota, node architecture, image name and registry pull credentials. |
| Driver reads metadata but executor receives storage `403` | Executor's own IAM/provider setup and permission for the referenced data, delete and index prefixes. |
| Native extraction/load failure | Writable temporary directory, hard-link support, executable shared-library mappings, matching OS/CPU bundle, JRE `libjsig` and one connector copy per JVM. |
| Tasks cannot connect to the driver or storage | Advertised addresses, DNS, CA trust, RPC/shuffle firewall rules and NetworkPolicy. |
| Container OOM with free JVM heap | Native/Python memory and executor overhead, index allocator settings, actual segment/index sizes and search budgets. |
| Options file missing in cluster mode | Its path in the driver container, Secret mount or dependency localization, not the submit host's filesystem. |

The Helm chart is a deployment template for the documented Spark version and
this connector's runtime. Image smoke tests and offline template checks do not
verify a live Kubernetes deployment. The
[recorded Milvus demo validation](../demo/live-validation.md#recorded-validation)
used local Spark with real object storage; it does not establish that your
namespace RBAC, cloud IAM or multi-node network configuration has been tested.
