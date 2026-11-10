# Run snapshot reads on Kubernetes

This chart submits the reader demos using native Spark on Kubernetes. Build a
current [reader image](../../../docker/README.md) and make it available to the
cluster. No Spark Operator is required. Match the image/native architecture to
`nodeSelector.kubernetes.io/arch`.

The default example is `read`. Every example requires an existing namespace-local
Secret containing `read-options.json`. Do not put credentials in Helm values.
Use a new Helm release name for each run; Job Pod specifications are immutable.

```yaml
image:
  repository: registry.example.com/spark-milvus-demo
  tag: current-amd64
language: python
example: read
input:
  existingSecret: milvus-read-options
  optionsKey: read-options.json
```

```bash
helm template snapshot-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo -f snapshot-demo.yaml
helm install snapshot-demo deploy/helm/spark-milvus-demo \
  --namespace spark-demo -f snapshot-demo.yaml
```

Supported examples are `read`, `filter`, `sql` and `partition`; `language: java`
selects their Java equivalents. The input Secret is mounted once on the driver.
Both driver and executors still need usable cloud identity and storage access.
See [cluster setup](../../../docs/cluster-setup.md) for permissions.

The chart sets explicit executor memory overhead and Java signal chaining. It
uses no connector extension or plugin. Driver/executor pod templates run as UID
185 with a read-only root filesystem and writable `/tmp` and Spark working
volumes. `extraSparkConf` accepts additional non-secret string properties but
cannot replace settings managed by the chart. The executor receives no
Kubernetes API Role; storage IAM is separate from Kubernetes RBAC.

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
helm lint deploy/helm/spark-milvus-demo --set input.existingSecret=read-input
python3 deploy/helm/spark-milvus-demo/tests/test_chart.py
```

The tests need Helm and PyYAML and render manifests without contacting Kubernetes.
Validation so far covers local lint and rendering; this chart has not been
deployed to a live Kubernetes cluster.
For the underlying behavior, see [Spark 4.0.1 Kubernetes documentation](https://spark.apache.org/docs/4.0.1/running-on-kubernetes.html),
the [executor template ConfigMap step](https://github.com/apache/spark/blob/v4.0.1/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/PodTemplateConfigMapStep.scala),
and the [submission status watcher](https://github.com/apache/spark/blob/v4.0.1/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/submit/LoggingPodStatusWatcher.scala).
