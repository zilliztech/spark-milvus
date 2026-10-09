# Spark-Milvus integration test chart

This chart runs an integration-test executable supplied by the selected image
in one Kubernetes `Job`. It can connect to an existing Milvus (the default),
or deploy a temporary standalone Milvus for this release. Test cases stay in
the selected private test image. The chart does not create object storage,
credentials, RBAC, persistent volumes, or a Spark operator.

## Required inputs

- An integration-test image selected by a `sha256` digest. The image must run
  with a numeric non-root user and provide
  `/opt/spark-milvus/bin/run-integration-tests`.
- A reachable Milvus endpoint, or `milvus.deployment.enabled: true` with an
  empty `milvus.uri` to deploy one. Both modes require an existing S3-compatible
  bucket and its endpoint, root path, region, and credentials.
- One existing Secret containing the Milvus token and one existing Secret
  containing the object-storage access key and secret key.

The image owns the runner executable. It must consume the environment variables
rendered by `templates/job.yaml`, run bounded integration checks, and return a
nonzero exit code when a check fails. A deployment-specific image may implement
the executable with a suite maintained outside this public repository. The
current public release image contains only the Connector assembly and does not
provide the executable, so it cannot be used with this chart as-is.

In existing-instance mode, `objectStorage.rootPath` is the target Milvus
instance's exact storage root. It is not changed or scoped to one CI run.
Test-owned output uses
`<objectStorage.writePrefix>/<runId>` through `MILVUS_UAT_WRITE_PREFIX`.

## Deploy a temporary Milvus

Use `ci-values.managed.example.yaml` for this mode. Set the test image digest,
existing Secret references, bucket settings, and your private test arguments.
The existing-instance example remains `ci-values.example.yaml`.

With `milvus.deployment.enabled: true`, the chart creates:

- One single-replica Deployment containing Milvus standalone and a private etcd
  container. RocksMQ runs inside Milvus. Versions default to Milvus 3.0.1 and
  etcd 3.5.25, matching the repository's dev environment; image references can
  be overridden with internal registry mirrors or digests. The default Milvus
  image is pinned to its linux/amd64 digest. Its numeric UID/GID is 999; update
  `milvus.deployment.runAsUser`/`runAsGroup` when a replacement image differs.
- One ClusterIP Service exposing the Milvus API and health endpoint; etcd
  listens only on the Pod's loopback interface.
- One ConfigMap containing non-secret Milvus settings. AK/SK and the initial
  root password are injected only through existing Secret references.

The chart automatically supplies the test and setup containers with the local
Milvus URI and the same S3 settings used by the server. In this mode the
storage root is `<rootPath>/ci/<namespace>/<release>/<runId>`. Use fresh release
and run IDs; this path must never overlap data belonging to another instance.
The endpoint must be a hostname with an optional port, without a URL scheme or
path; the default port is 443 with SSL and 80 otherwise.

The Milvus Secret must contain `milvus.credentials.rootPasswordKey` (default
`root-password`) and `tokenKey`. The token must be `root:<the same password>`.
Create these through the existing CI secret-management path. The chart does
not create or read Secret values. It enables Milvus authentication.

The Job waits for Milvus `/healthz` with a bounded init container. It then runs
`runner.setup.command` and `runner.setup.args`, when supplied, in the **same
private image** as the test. This command receives the same environment and
Secret references and must exit successfully before the test starts. The
Job's overall deadline also bounds setup. A replacement Milvus image must
include `/bin/sh`, `date`, `sleep`, and `curl` for the readiness check.

**A new Milvus is empty.** The existing private read-only smoke runner does
not create collections, insert data, flush, or create snapshots. Before using
that runner in managed mode, provide a preparation command in the private
image that creates the selected collection, inserts and flushes bounded test
data, and waits for a readable snapshot. For a non-default database it must
create that database too. Configure that command through `runner.setup`, or
use a private runner that owns its entire fixture lifecycle. The chart does
not supply the command or embed business cases; enabling Milvus deployment
alone does not make the read-only smoke pass.

Milvus and etcd data use `emptyDir` and are lost when the Pod is replaced.
This mode is for CI, not persistent environments. Helm uninstall removes the
Deployment, Service, ConfigMap, and Job. It does not delete objects in S3:
use private fixture cleanup or a bucket lifecycle policy for test prefixes.
The Job TTL does not delete the Deployment; CI infrastructure must reclaim an
orphaned release if the runner is killed before its cleanup trap executes.

## Credentials

The chart accepts only Secret names and Secret key names. It has no values for
raw tokens or access keys and never creates a Kubernetes `Secret`.

| Secret reference | Required key | Container variable |
|---|---|---|
| `milvus.credentials` | `tokenKey` | `MILVUS_UAT_TOKEN` |
| `milvus.credentials` (managed only) | `rootPasswordKey` | `COMMON_SECURITY_DEFAULTROOTPASSWORD` |
| `objectStorage.credentials` | `accessKeyIdKey` | `AWS_ACCESS_KEY_ID` |
| `objectStorage.credentials` | `secretAccessKeyKey` | `AWS_SECRET_ACCESS_KEY` |

Provision these Secrets through the CI cluster's secret-management path. Do
not put their values in a Helm values file or command-line argument.

## Validate the chart

Copy `ci-values.example.yaml` or `ci-values.managed.example.yaml` outside the
chart, replace its non-secret placeholders, and run:

```bash
helm lint --strict deploy/helm/spark-milvus-integration \
  --values /path/to/ci-values.yaml

helm template spark-milvus-it deploy/helm/spark-milvus-integration \
  --namespace spark-milvus-it \
  --values /path/to/ci-values.yaml
```

Run the template and lifecycle tests locally (Python tests require PyYAML).
They render both modes and use local `helm`/`kubectl` fakes, without contacting
a cluster:

```bash
python3 -m unittest discover -s deploy/helm/spark-milvus-integration/tests -p 'test_*.py'
deploy/helm/spark-milvus-integration/tests/run-test.sh
```

Empty image, endpoint, bucket, or Secret references fail schema validation.

## Run in CI

Create the namespace and credential Secrets through the CI cluster's existing
provisioning path. The script deliberately does not create the namespace: the
required Secrets must already exist there. Use a unique release name for every
CI run:

```bash
CI_RUN_ID="${CI_RUN_ID}" \
  deploy/helm/spark-milvus-integration/run.sh \
  "spark-milvus-it-${CI_RUN_ID}" \
  spark-milvus-it \
  /path/to/ci-values.yaml
```

The script lints the chart, verifies the namespace and release name, installs
without `--atomic`, and watches both successful and failed Job conditions. It
also fails immediately when a Pod reports `ErrImagePull`, `ImagePullBackOff`,
`ErrImageNeverPull`, `InvalidImageName`, `CreateContainerConfigError`,
`RunContainerError`, `CrashLoopBackOff`, or an `Unschedulable` scheduling
condition, including Milvus/etcd and the Job's init containers. It collects
Helm status, test and Milvus/etcd logs, descriptions, and events
under `target/integration-helm/<release>/`, and then uninstalls the release. Set
`KEEP_RESOURCES=true` only when a failed run must remain for investigation.
`JOB_WAIT_TIMEOUT_SECONDS` defaults to `3900`, slightly longer than the Job's
one-hour deadline. Kubernetes API calls use a bounded request timeout, and Helm
cleanup uses `HELM_OPERATION_TIMEOUT`, which defaults to `5m`.

The CI identity needs read access to Jobs, Pods, and Events, permission to
create and delete this release's Job, and Helm's normal release-metadata
permissions. Managed mode additionally needs create/read/delete access to
Deployments, Services, and ConfigMaps; Pod diagnostics cover the whole release.
With Helm's default storage driver, that metadata is stored in
Kubernetes Secrets, so the namespace role must include Helm's required Secret
operations. The script does not fetch credential Secret values itself; missing
Secret names or keys surface as `CreateContainerConfigError` and terminate the
wait early.

Kubernetes retries are disabled with `backoffLimit: 0`. A failed test is a
failed Job; rerunning it requires a new release and run ID. If a successful
test cannot be uninstalled, the script also fails so CI cannot silently leak a
release.
