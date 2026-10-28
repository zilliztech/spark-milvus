"""Render Helm manifests offline and verify Spark submission and access boundaries."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parents[1]
EXAMPLES = {
    "read": ("01_read_snapshot.py", "ReadSnapshot"),
    "filter": ("02_filter_project.py", "FilterProject"),
    "sql": ("03_sql_analytics.py", "SqlAnalytics"),
    "exact": ("04_exact_search.py", "ExactSearch"),
    "filtered": ("05_filtered_search.py", "FilteredSearch"),
    "index": ("06_index_search.py", "IndexSearch"),
    "vectors": ("07_vector_functions.py", "VectorFunctions"),
    "partition": ("08_partition_selection.py", "PartitionSelection"),
}


def render(overrides=None, release="demo-a", namespace="demo-tests"):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as values:
        yaml.safe_dump(overrides or {}, values)
        values.flush()
        return subprocess.run(["helm", "template", release, str(CHART), "--namespace", namespace,
                               "--values", values.name], text=True, capture_output=True,
                              env=dict(os.environ, KUBECONFIG="/nonexistent/offline-kubeconfig"), timeout=20)


def resources(**kwargs):
    result = render(**kwargs)
    if result.returncode:
        raise AssertionError(result.stderr)
    return [resource for resource in yaml.safe_load_all(result.stdout) if resource]


def resource(docs, kind):
    return next(doc for doc in docs if doc["kind"] == kind)


def pod(job):
    return job["spec"]["template"]["spec"]


def arguments(docs):
    return pod(resource(docs, "Job"))["containers"][0]["args"]


def configuration(docs):
    args = arguments(docs)
    return dict(args[index + 1].split("=", 1) for index, arg in enumerate(args) if arg == "--conf")


class ChartTest(unittest.TestCase):
    def test_default_chart_submits_vectors_without_storage_secret_or_operator(self):
        docs = resources()
        self.assertEqual(sorted(doc["kind"] for doc in docs),
                         ["ConfigMap", "Job", "Role", "RoleBinding", "ServiceAccount", "ServiceAccount", "ServiceAccount"])
        job = resource(docs, "Job")
        spec = pod(job)
        args = arguments(docs)
        self.assertEqual(args[:5], ["/opt/spark/bin/spark-submit", "--master", "k8s://https://kubernetes.default.svc:443",
                                    "--deploy-mode", "cluster"])
        self.assertNotIn("command", spec["containers"][0], "retain the Spark image's entrypoint")
        self.assertNotIn("--options", args)
        self.assertNotIn("--queries", args)
        self.assertTrue(all("secret" not in volume for volume in spec["volumes"]))
        self.assertTrue(all(doc["metadata"]["namespace"] == "demo-tests" for doc in docs))
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        self.assertEqual(job["spec"]["activeDeadlineSeconds"], 1800)
        self.assertEqual(spec["containers"][0]["resources"]["requests"]["memory"], "1Gi")
        self.assertEqual(spec["containers"][0]["resources"]["limits"]["memory"], "2Gi")
        self.assertEqual(spec["restartPolicy"], "Never")

    def test_every_language_and_example_selects_only_baked_application_files(self):
        for language in ("python", "java"):
            for example, (python_file, java_class) in EXAMPLES.items():
                with self.subTest(language=language, example=example):
                    docs = resources(overrides={"language": language, "example": example,
                                                "input": {"existingSecret": "snapshot-input"}})
                    args = arguments(docs)
                    self.assertEqual(args[args.index("--jars") + 1], "local:///opt/spark-milvus/connector.jar")
                    if language == "python":
                        self.assertEqual(args[args.index("--py-files") + 1],
                                         "local:///opt/spark-milvus/demo/python/common.py")
                        self.assertIn("local:///opt/spark-milvus/demo/python/" + python_file, args)
                    else:
                        self.assertEqual(args[args.index("--class") + 1],
                                         "com.zilliz.spark.connector.demo." + java_class)
                        self.assertIn("local:///opt/spark-milvus/demo-java.jar", args)
                    if example == "vectors":
                        self.assertNotIn("--options", args)
                        self.assertFalse(any(key.startswith("spark.kubernetes.driver.secrets.")
                                             for key in configuration(docs)))
                    else:
                        self.assertEqual(args[args.index("--options") + 1], "/etc/spark-milvus/input/read-options.json")
                    if example in ("exact", "filtered", "index"):
                        self.assertEqual(args[args.index("--queries") + 1], "/etc/spark-milvus/input/queries.json")
                    else:
                        self.assertNotIn("--queries", args)

    def test_snapshot_and_query_keys_share_one_driver_mount(self):
        docs = resources(overrides={"example": "exact", "input": {"existingSecret": "existing-input",
                                                                  "optionsKey": "options.json", "queriesKey": "vectors.json"}})
        conf = configuration(docs)
        mounts = {key: value for key, value in conf.items() if ".secrets." in key}
        self.assertEqual(mounts, {"spark.kubernetes.driver.secrets.existing-input": "/etc/spark-milvus/input"})
        self.assertEqual(arguments(docs)[-4:], ["--options", "/etc/spark-milvus/input/options.json",
                                               "--queries", "/etc/spark-milvus/input/vectors.json"])
        self.assertNotIn("Secret", [doc["kind"] for doc in docs])
        self.assertFalse(any("secret" in volume for volume in pod(resource(docs, "Job"))["volumes"]))

    def test_baked_queries_require_an_explicit_empty_query_key(self):
        docs = resources(overrides={"example": "exact", "input": {"existingSecret": "existing-input", "queriesKey": ""}})
        self.assertEqual(arguments(docs)[-2:], ["--queries", "/opt/spark-milvus/demo/config/queries.json"])

    def test_snapshot_examples_require_an_existing_secret(self):
        for example in EXAMPLES:
            if example != "vectors":
                with self.subTest(example=example):
                    result = render({"example": example})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("input.existingSecret is required", result.stderr)

    def test_scoped_rbac_binds_submitter_and_driver_but_not_executor(self):
        docs = resources()
        role = resource(docs, "Role")
        binding = resource(docs, "RoleBinding")
        self.assertEqual({item for rule in role["rules"] for item in rule["resources"]},
                         {"pods", "services", "configmaps"})
        self.assertEqual(set(role["rules"][0]["verbs"]),
                         {"create", "get", "list", "watch", "patch", "delete", "deletecollection"})
        names = {subject["name"] for subject in binding["subjects"]}
        conf = configuration(docs)
        self.assertEqual(names, {pod(resource(docs, "Job"))["serviceAccountName"],
                                 conf["spark.kubernetes.authenticate.driver.serviceAccountName"]})
        self.assertNotIn(conf["spark.kubernetes.authenticate.executor.serviceAccountName"], names)
        self.assertTrue(pod(resource(docs, "Job"))["automountServiceAccountToken"])
        self.assertEqual(conf["spark.kubernetes.authenticate.submission.oauthTokenFile"],
                         "/var/run/secrets/kubernetes.io/serviceaccount/token")
        self.assertEqual(conf["spark.kubernetes.authenticate.submission.caCertFile"],
                         "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

    def test_existing_service_accounts_and_platform_rbac_are_preserved(self):
        overrides = {"rbac": {"create": False}, "serviceAccounts": {
            role: {"create": False, "name": "existing-" + role} for role in ("submitter", "driver", "executor")}}
        docs = resources(overrides=overrides)
        self.assertEqual(sorted(doc["kind"] for doc in docs), ["ConfigMap", "Job"])
        self.assertEqual(pod(resource(docs, "Job"))["serviceAccountName"], "existing-submitter")
        conf = configuration(docs)
        self.assertEqual(conf["spark.kubernetes.authenticate.driver.serviceAccountName"], "existing-driver")
        self.assertEqual(conf["spark.kubernetes.authenticate.executor.serviceAccountName"], "existing-executor")
        for role in ("submitter", "driver", "executor"):
            with self.subTest(role=role):
                result = render({"serviceAccounts": {role: {"create": False}}})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("name is required when create=false", result.stderr)

    def test_workload_identity_annotations_are_per_account(self):
        docs = resources(overrides={"serviceAccounts": {"driver": {"annotations": {"example.com/role": "driver-reader"}},
                                                       "executor": {"annotations": {"example.com/role": "executor-reader"}}}})
        accounts = {doc["metadata"]["name"]: doc for doc in docs if doc["kind"] == "ServiceAccount"}
        conf = configuration(docs)
        driver = accounts[conf["spark.kubernetes.authenticate.driver.serviceAccountName"]]
        executor = accounts[conf["spark.kubernetes.authenticate.executor.serviceAccountName"]]
        self.assertEqual(driver["metadata"]["annotations"]["example.com/role"], "driver-reader")
        self.assertEqual(executor["metadata"]["annotations"]["example.com/role"], "executor-reader")

    def test_restricted_pod_templates_keep_working_data_separate_from_image_jars(self):
        docs = resources()
        configmap = resource(docs, "ConfigMap")
        driver = yaml.safe_load(configmap["data"]["driver.yaml"])["spec"]
        executor = yaml.safe_load(configmap["data"]["executor.yaml"])["spec"]
        submitter = pod(resource(docs, "Job"))
        for spec in (submitter, driver, executor):
            self.assertTrue(spec["securityContext"]["runAsNonRoot"])
            self.assertEqual(spec["securityContext"]["runAsUser"], 185)
            self.assertEqual(spec["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
            container = spec["containers"][0]
            self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
            self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
            self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])
            mounts = {item["name"]: item["mountPath"] for item in container["volumeMounts"]}
            self.assertEqual(mounts["tmp"], "/tmp")
            self.assertEqual(mounts["work-dir"], "/opt/spark/work-dir")
            self.assertNotIn("/opt/spark-milvus", mounts.values())
            self.assertEqual([volume["name"] for volume in spec["volumes"] if "emptyDir" in volume], ["work-dir", "tmp"])
        self.assertTrue(driver["automountServiceAccountToken"])
        self.assertFalse(executor["automountServiceAccountToken"])
        self.assertEqual(submitter["containers"][0]["workingDir"], "/opt/spark/work-dir")
        conf = configuration(docs)
        self.assertEqual(conf["spark.kubernetes.driver.podTemplateFile"], "/etc/spark-milvus/templates/driver.yaml")
        self.assertEqual(conf["spark.kubernetes.executor.podTemplateFile"], "/etc/spark-milvus/templates/executor.yaml")

    def test_native_runtime_and_no_pvc_settings_are_explicit(self):
        conf = configuration(resources())
        for key in ("spark.kubernetes.driver.ownPersistentVolumeClaim", "spark.kubernetes.driver.reusePersistentVolumeClaim",
                    "spark.dynamicAllocation.enabled"):
            self.assertEqual(conf[key], "false")
        self.assertEqual(conf["spark.kubernetes.submission.waitAppCompletion"], "true")
        for prefix in ("spark.kubernetes.driverEnv", "spark.executorEnv"):
            self.assertEqual(conf[prefix + ".LD_PRELOAD"], "/opt/java/openjdk/lib/libjsig.so")
            self.assertEqual(conf[prefix + ".LD_BIND_NOW"], "1")
            self.assertEqual(conf[prefix + ".MALLOC_ARENA_MAX"], "2")
        self.assertEqual(conf["spark.plugins"], "com.zilliz.spark.connector.extensions.MilvusSparkPlugin")

    def test_image_digest_architecture_and_resources_reach_all_processes(self):
        digest = "sha256:" + "a" * 64
        docs = resources(overrides={"image": {"repository": "registry.invalid/demo", "digest": digest, "pullSecrets": ["registry-key"]},
                                    "nodeSelector": {"kubernetes.io/arch": "arm64", "nodepool": "native"},
                                    "spark": {"executor": {"memory": "6g", "memoryOverhead": "3g", "instances": 3, "cores": 1}}})
        spec = pod(resource(docs, "Job"))
        self.assertEqual(spec["containers"][0]["image"], "registry.invalid/demo@" + digest)
        self.assertEqual(spec["imagePullSecrets"], [{"name": "registry-key"}])
        conf = configuration(docs)
        self.assertEqual(conf["spark.kubernetes.container.image"], spec["containers"][0]["image"])
        self.assertEqual(conf["spark.kubernetes.container.image.pullSecrets"], "registry-key")
        self.assertEqual(spec["nodeSelector"]["kubernetes.io/arch"], "arm64")
        self.assertEqual(conf["spark.kubernetes.node.selector.kubernetes.io/arch"], "arm64")
        self.assertEqual(conf["spark.kubernetes.node.selector.nodepool"], "native")
        self.assertEqual(conf["spark.executor.memory"], "6g")
        self.assertEqual(conf["spark.executor.memoryOverhead"], "3g")
        self.assertEqual(conf["spark.executor.instances"], "3")
        self.assertEqual(conf["spark.executor.cores"], "1")

    def test_schema_rejects_invalid_names_paths_types_and_inline_secret_fields(self):
        cases = [
            {"language": "scala"}, {"example": "../../outside.py"},
            {"input": {"existingSecret": "read.options"}}, {"input": {"existingSecret": "s" * 57}},
            {"input": {"optionsKey": "../options.json"}}, {"input": {"queriesKey": "directory/query.json"}},
            {"input": {"contents": "PRIVATE_CREDENTIAL"}}, {"input": {"token": "PRIVATE_CREDENTIAL"}},
            {"credentials": {"secret": "PRIVATE_CREDENTIAL"}}, {"image": {"repository": "bad image"}},
            {"image": {"digest": "latest"}}, {"image": {"tag": "", "digest": ""}},
            {"job": {"activeDeadlineSeconds": 0}}, {"spark": {"executor": {"instances": 0}}},
            {"serviceAccounts": {"driver": {"name": "bad/name"}}}, {"nodeSelector": {"kubernetes.io/arch": "x86_64"}},
            {"extraSparkConf": {"spark.sql.shuffle.partitions": 2}},
        ]
        for values in cases:
            with self.subTest(values=values):
                self.assertNotEqual(render(values).returncode, 0)
        self.assertEqual(render({"example": "read", "input": {"existingSecret": "s" * 56}}).returncode, 0)

    def test_extra_configuration_cannot_replace_managed_target_or_application_settings(self):
        docs = resources(overrides={"extraSparkConf": {"spark.sql.shuffle.partitions": "7"}})
        self.assertEqual(configuration(docs)["spark.sql.shuffle.partitions"], "7")
        for key in ("spark.kubernetes.namespace", "spark.kubernetes.container.image", "spark.kubernetes.driver.podTemplateFile",
                    "spark.kubernetes.authenticate.driver.serviceAccountName", "spark.plugins", "spark.executor.memory"):
            with self.subTest(key=key):
                result = render({"extraSparkConf": {key: "unmanaged-value"}})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("cannot override chart-managed setting", result.stderr)

    def test_release_names_and_labels_isolate_long_similar_releases(self):
        names = set()
        for release in ("a", "b", "r" * 52 + "a", "r" * 52 + "b"):
            docs = resources(release=release, namespace="chosen-namespace")
            job = resource(docs, "Job")
            names.add(job["metadata"]["name"])
            for doc in docs:
                self.assertLessEqual(len(doc["metadata"]["name"]), 63)
                self.assertEqual(doc["metadata"]["namespace"], "chosen-namespace")
                self.assertEqual(doc["metadata"]["labels"]["app.kubernetes.io/instance"], release)
            conf = configuration(docs)
            self.assertEqual(conf["spark.kubernetes.driver.label.app.kubernetes.io/instance"], release)
            self.assertEqual(conf["spark.kubernetes.executor.label.app.kubernetes.io/instance"], release)
            self.assertLessEqual(len(conf["spark.kubernetes.driver.pod.name"]), 63)
        self.assertEqual(len(names), 4)


if __name__ == "__main__":
    unittest.main()
