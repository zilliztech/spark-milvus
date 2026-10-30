# Spark-Milvus demo on Kubernetes

This Helm chart runs one Python or Java demo on an existing Kubernetes cluster.
A Job runs Spark 4.0.1's `spark-submit` inside the application image; Spark creates
the driver and executors. No Spark Operator or separately deployed Spark master
is required. Use the published `milvusdb/spark-milvus:2.0.0-spark4.0` image
or build one with the [image guide](../../../docker/README.md), and review the
[cluster permissions and storage setup](../../../docs/cluster-setup.md).

The chart does not call Milvus, create snapshots, create/read input Secrets, or
provision a Kubernetes cluster. `helm template` and `helm lint` work offline.

## Render and install

Start with the vector-functions example, which needs no snapshot:

```bash
helm lint deploy/helm/spark-milvus-demo
helm template vector-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo \
  --set image.repository=milvusdb/spark-milvus \
  --set image.tag=2.0.0-spark4.0

helm install vector-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo --create-namespace \
  --set image.repository=milvusdb/spark-milvus \
  --set image.tag=2.0.0-spark4.0
```

The chart defaults to a locally built image, so the examples explicitly select
the published image. The cluster nodes must be able to pull it. For immutable
image selection, set `image.digest=sha256:<64 lowercase hex characters>`; it takes precedence over
the tag. `image.pullSecrets` is a list of existing registry Secret names and is
applied to the submitter, driver and executors. Match the native bundle and image
to `nodeSelector.kubernetes.io/arch` (`amd64` by default, or `arm64`).

Use a new Helm release name for each run. Kubernetes Job Pod specifications are
immutable, and the chart uses a fixed driver name within each release.

## Existing snapshot inputs

An operator supplies an existing Secret in the release namespace with these keys:

| Key | Contents |
|---|---|
| `read-options.json` | Connector options including `milvus.snapshot.path` and storage settings. |
| `queries.json` | Query-vector JSON for `exact`, `filtered` and `index`. |

Do not put credentials or Secret contents in Helm values: Helm stores its values
and rendered manifests. The Secret name must be a DNS label of at most 56
characters because Spark adds a suffix when naming its Pod volume. Use
`input.optionsKey` and `input.queriesKey` to select different keys.

For example, save these non-secret values as `snapshot-demo.yaml`:

```yaml
image:
  repository: milvusdb/spark-milvus
  tag: 2.0.0-spark4.0
language: python
example: exact
input:
  existingSecret: milvus-demo-input
  optionsKey: read-options.json
  queriesKey: queries.json
```

```bash
helm template snapshot-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo -f snapshot-demo.yaml
helm install snapshot-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo -f snapshot-demo.yaml
```

The input Secret is mounted once in the driver at `/etc/spark-milvus/input`.
Neither the submission Job nor executors need this JSON-file mount; the driver
passes connector options to executors. Cloud identity, custom CA files and any
provider token files required by native storage must still be usable in both
driver and executor. Configure their ServiceAccounts' IAM identities separately.

Search examples use the Secret's `queries.json` by default. Set
`input.queriesKey: ""` only when the baked two-dimensional example queries match
the snapshot. The `vectors` example mounts no input Secret and passes no snapshot
options. Read/filter/SQL/partition examples do not use query vectors.

## Examples and settings

| `example` | Python | Java |
|---|---|---|
| `read` | `01_read_snapshot.py` | `ReadSnapshot` |
| `filter` | `02_filter_project.py` | `FilterProject` |
| `sql` | `03_sql_analytics.py` | `SqlAnalytics` |
| `exact` | `04_exact_search.py` | `ExactSearch` |
| `filtered` | `05_filtered_search.py` | `FilteredSearch` |
| `index` | `06_index_search.py` | `IndexSearch` |
| `vectors` | `07_vector_functions.py` | `VectorFunctions` |
| `partition` | `08_partition_selection.py` | `PartitionSelection` |

Set `language: java` to select Java. See the [demo walkthrough](../../../demo/README.md)
for required columns, query dimensions, scalar filters and index compatibility.
The chart uses each example's default application arguments.

The [values file](values.yaml) contains Spark driver/executor heap, native memory
overhead, cores, executor count, submitter Job resources and deadline. Defaults
use two executors with 4 GiB heap plus 4 GiB overhead each; adjust for your workload
and namespace quota. `extraSparkConf` accepts non-secret string properties, such
as an additional Secret mount or scheduler setting. It cannot replace settings
managed by the chart. Storage credential values belong in existing Secrets or
the workload identity provider, not in this map.

The chart disables dynamic allocation and driver PVC ownership/reuse. Its
ConfigMap supplies driver and executor Pod templates. Spark reads the driver
template in the submitter and copies the executor template into its own ConfigMap
for the driver; no object-store staging location is required. All three process
types run as UID/GID 185 with `runAsNonRoot`, dropped capabilities, no privilege
escalation, `RuntimeDefault` seccomp and read-only root filesystems. Separate
`emptyDir` volumes make `/tmp` and `/opt/spark/work-dir` writable; application JARs
remain under `/opt/spark-milvus`. Native memory allocations count toward Pod
limits even though they are outside the JVM heap.

## Accounts and permissions

Installation requires permission to create the chart's Job and ConfigMap, plus
ServiceAccounts and namespace-scoped RBAC when their creation flags are enabled.
`--create-namespace` additionally requires namespace creation permission. The chart
does not create a namespace resource itself. The installer also needs Helm's
release-storage permissions (Helm uses Secrets by default); these are separate
from the runtime accounts and the existing input Secret.

The submitter and driver ServiceAccounts receive a scoped baseline allowing
`create/get/list/watch/patch/delete/deletecollection` on Pods, Services and
ConfigMaps. This covers submission and cleanup with the chart's fixed allocation
and no-PVC settings; it is not a universal minimum for additional Spark features.
The executor account receives no Kubernetes API Role. None of these Roles grants
Secret read/create access. The submitter uses its projected Kubernetes token and
CA to reach the in-cluster API and does not need a storage IAM identity for these
image-local application files. Driver and executor storage IAM permissions are
independent of Kubernetes RBAC.

To use existing accounts and platform-managed RBAC:

```yaml
rbac:
  create: false
serviceAccounts:
  submitter:
    create: false
    name: spark-submitter
  driver:
    create: false
    name: spark-driver
  executor:
    create: false
    name: spark-executor
```

When creating accounts, `serviceAccounts.<role>.annotations` can carry the
platform's workload-identity annotations. Setting annotations in values does not
modify an account whose `create` flag is false. Provision its IAM trust and
annotations outside this chart. The executor template disables automatic
Kubernetes API-token mounting; provider-specific projected identity tokens can
still be injected by the platform.

## Check the application

The install output names the Job and driver. Check driver phase and logs using
the release label, for example:

```bash
kubectl -n spark-demo get pods \
  -l 'spark-role=driver,app.kubernetes.io/instance=snapshot-demo' \
  -o custom-columns=NAME:.metadata.name,PHASE:.status.phase
kubectl -n spark-demo logs \
  -l 'spark-role=driver,app.kubernetes.io/instance=snapshot-demo' --tail=-1
```

The submission Job waits for the driver to reach a terminal phase. **A completed
Job or successful `helm --wait --wait-for-jobs` is not proof of application
success:** Spark 4.0.1's submission watcher also returns after a failed driver.
The driver must report `Succeeded`, with the example's expected output in its
logs. If the Job deadline expires, inspect the driver separately; ending the
submitter does not guarantee that the application stopped.

Helm owns the Job, template ConfigMap, and any accounts/RBAC it created. Spark
owns its application resources. After application completion, inspect the
release-labeled Pods and related resources before targeted cleanup or uninstall.
Do not uninstall an active run's driver Role or ServiceAccount. The chart has no
cleanup hook and never deletes the namespace or existing input Secrets.

## Validate templates

```bash
helm lint deploy/helm/spark-milvus-demo
python3 deploy/helm/spark-milvus-demo/tests/test_chart.py
```

The tests need Helm and PyYAML and render manifests without contacting Kubernetes.
Validation so far covers local lint and rendering; this chart has not been
deployed to a live Kubernetes cluster.
For the underlying behavior, see [Spark 4.0.1 Kubernetes documentation](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html),
the [executor template ConfigMap step](https://github.com/apache/spark/blob/v4.0.1/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/PodTemplateConfigMapStep.scala),
and the [submission status watcher](https://github.com/apache/spark/blob/v4.0.1/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/submit/LoggingPodStatusWatcher.scala).
