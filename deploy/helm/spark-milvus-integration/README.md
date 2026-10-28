# Fixed V3 read integration Job

This chart runs the public reader against immutable V3 files in an existing
object store. It renders one Job and does not deploy Milvus or prepare data.

Prepare `collections/nearest` and two independently authorized identities as
specified in [the contributor guide](../../../docs/contributing.md#tests-that-need-the-native-library).
Create an existing Kubernetes Secret containing `a.json` and `b.json`, each
holding the snapshot path and its own `fs.*` storage configuration. The runner
receives `MILVUS_READ_IT_OPTIONS` and `MILVUS_READ_IT_OPTIONS_B` as separate paths
under a read-only Secret mount. It must validate both authorized reads and
cross-identity failures. Credentials are never supplied in Helm values.

Copy [ci-values.read.example.yaml](ci-values.read.example.yaml), select a real
read-test image by immutable digest and set `readOptions.existingSecret`.
The image must provide `runner.command`; the local bundle-only Docker image
does not contain an integration runner.

```bash
helm lint --strict deploy/helm/spark-milvus-integration -f /private/read-values.yaml
deploy/helm/spark-milvus-integration/run.sh read-check tests /private/read-values.yaml
```

`run.sh` checks ownership before cleanup, retains diagnostics and never replaces
an existing Helm release. It uninstalls only the release it created. Input
objects and the externally managed Secret remain owned by their preparer.
`KEEP_RESOURCES=true` preserves the test release for diagnosis; `ARTIFACT_DIR`
selects the local diagnostic directory. Test failures remain failures even when
cleanup succeeds.

`image.digest` and the Secret name are required. Old `milvus`, `objectStorage`
and `runner.setup` settings are rejected instead of silently accepted. Job
security, service account, resources, deadline and architecture are explicit
values. Validate templates and cleanup behavior without a Kubernetes cluster:

```bash
python3 -m unittest discover -s deploy/helm/spark-milvus-integration/tests -p 'test_*.py'
bash deploy/helm/spark-milvus-integration/tests/run-test.sh
```

Template tests prove configuration and ownership handling, not an executed
cluster job. Runtime read acceptance requires the real runner and object store.
