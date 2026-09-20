# Spark-Milvus integration test chart

This chart runs an integration-test executable supplied by the selected image
in one Kubernetes `Job`. The chart does not prescribe the test framework or
where the suite is maintained, and it does not install Milvus, object storage,
credentials, RBAC, or a Spark operator.

## Required inputs

- An integration-test image selected by a `sha256` digest. The image must run
  with a numeric non-root user and provide
  `/opt/spark-milvus/bin/run-integration-tests`.
- A reachable Milvus endpoint and the object-storage endpoint, bucket, root
  path, and region used by that Milvus deployment.
- One existing Secret containing the Milvus token and one existing Secret
  containing the object-storage access key and secret key.

The image owns the runner executable. It must consume the environment variables
rendered by `templates/job.yaml`, run bounded integration checks, and return a
nonzero exit code when a check fails. A deployment-specific image may implement
the executable with a suite maintained outside this public repository. The
current public release image contains only the Connector assembly and does not
provide the executable, so it cannot be used with this chart as-is.

`objectStorage.rootPath` is the target Milvus deployment's storage root. It is
not scoped to one CI run. Test-owned output uses
`<objectStorage.writePrefix>/<runId>` through `MILVUS_UAT_WRITE_PREFIX`.

## Credentials

The chart accepts only Secret names and Secret key names. It has no values for
raw tokens or access keys and never creates a Kubernetes `Secret`.

| Secret reference | Required key | Container variable |
|---|---|---|
| `milvus.credentials` | `tokenKey` | `MILVUS_UAT_TOKEN` |
| `objectStorage.credentials` | `accessKeyIdKey` | `AWS_ACCESS_KEY_ID` |
| `objectStorage.credentials` | `secretAccessKeyKey` | `AWS_SECRET_ACCESS_KEY` |

Provision these Secrets through the CI cluster's secret-management path. Do
not put their values in a Helm values file or command-line argument.

## Validate the chart

Copy `ci-values.example.yaml` outside the chart, replace its non-secret
placeholders, and run:

```bash
helm lint --strict deploy/helm/spark-milvus-integration \
  --values /path/to/ci-values.yaml

helm template spark-milvus-it deploy/helm/spark-milvus-integration \
  --namespace spark-milvus-it \
  --values /path/to/ci-values.yaml
```

Run the lifecycle shell tests separately; they use local `helm` and `kubectl`
fakes and do not contact a cluster:

```bash
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
`RunContainerError`, or an `Unschedulable` scheduling condition. It collects
Helm status, logs, descriptions, and events
under `target/integration-helm/<release>/`, and then uninstalls the release. Set
`KEEP_RESOURCES=true` only when a failed run must remain for investigation.
`JOB_WAIT_TIMEOUT_SECONDS` defaults to `3900`, slightly longer than the Job's
one-hour deadline. Kubernetes API calls use a bounded request timeout, and Helm
cleanup uses `HELM_OPERATION_TIMEOUT`, which defaults to `5m`.

The CI identity needs read access to Jobs, Pods, and Events, permission to
create and delete this release's Job, and Helm's normal release-metadata
permissions. With Helm's default storage driver, that metadata is stored in
Kubernetes Secrets, so the namespace role must include Helm's required Secret
operations. The script does not fetch credential Secret values itself; missing
Secret names or keys surface as `CreateContainerConfigError` and terminate the
wait early.

Kubernetes retries are disabled with `backoffLimit: 0`. A failed test is a
failed Job; rerunning it requires a new release and run ID. If a successful
test cannot be uninstalled, the script also fails so CI cannot silently leak a
release.
