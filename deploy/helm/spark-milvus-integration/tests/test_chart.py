"""Render the external/managed deployment contracts without a Kubernetes cluster."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parents[1]


def render(managed=False, release="test-a", namespace="tests", overrides=None):
    example = "ci-values.managed.example.yaml" if managed else "ci-values.example.yaml"
    command = ["helm", "template", release, str(CHART), "--namespace", namespace,
               "--values", str(CHART / example)]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as values:
        yaml.safe_dump(overrides or {}, values)
        values.flush()
        return subprocess.run(command + ["--values", values.name], text=True,
                              capture_output=True)


def resources(**kwargs):
    result = render(**kwargs)
    if result.returncode:
        raise AssertionError(result.stderr)
    return {doc["kind"]: doc for doc in yaml.safe_load_all(result.stdout) if doc}


def pod(resource):
    return resource["spec"]["template"]["spec"]


def environment(container):
    return {entry["name"]: entry for entry in container["env"]}


class ChartTest(unittest.TestCase):
    def test_external_instance_keeps_its_uri_and_storage_root(self):
        docs = resources()
        self.assertEqual(set(docs), {"Job"})
        spec = pod(docs["Job"])
        self.assertNotIn("initContainers", spec)
        env = environment(spec["containers"][0])
        self.assertEqual(env["MILVUS_UAT_URI"]["value"],
                         "http://milvus-proxy.milvus.svc:19530")
        self.assertEqual(env["MILVUS_JNI_S3_ROOT_PATH"]["value"], "files")

    def test_managed_server_and_runner_use_the_same_storage_and_secret_references(self):
        docs = resources(managed=True)
        self.assertEqual(set(docs), {"Job", "Deployment", "Service", "ConfigMap"})
        server = pod(docs["Deployment"])
        runner = pod(docs["Job"])
        env = environment(runner["containers"][0])
        config = yaml.safe_load(docs["ConfigMap"]["data"]["user.yaml"])
        self.assertEqual(config["minio"]["rootPath"],
                         env["MILVUS_JNI_S3_ROOT_PATH"]["value"])
        self.assertEqual(config["minio"]["bucketName"],
                         env["MILVUS_JNI_S3_BUCKET"]["value"])
        self.assertEqual(config["minio"]["port"], 443)
        self.assertEqual(env["MILVUS_UAT_URI"]["value"],
                         "http://" + docs["Service"]["metadata"]["name"] + ":19530")
        self.assertTrue(config["common"]["security"]["authorizationEnabled"])
        self.assertEqual(server["securityContext"]["runAsUser"], 999)
        self.assertEqual([c["name"] for c in server["containers"]], ["milvus", "etcd"])
        server_env = environment(server["containers"][0])
        for source, target in [("MINIO_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"),
                               ("MINIO_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY")]:
            self.assertEqual(server_env[source]["valueFrom"], env[target]["valueFrom"])
        password = server_env["COMMON_SECURITY_DEFAULTROOTPASSWORD"]
        self.assertNotIn("value", password)
        self.assertEqual(password["valueFrom"]["secretKeyRef"]["key"], "root-password")
        self.assertNotIn("accessKeyID", config["minio"])
        self.assertNotIn("secretAccessKey", config["minio"])
        self.assertFalse(server["automountServiceAccountToken"])
        self.assertEqual(docs["Service"]["spec"]["type"], "ClusterIP")
        self.assertEqual({p["port"] for p in docs["Service"]["spec"]["ports"]}, {19530, 9091})
        self.assertEqual(docs["Service"]["spec"]["selector"],
                         docs["Deployment"]["spec"]["selector"]["matchLabels"])

    def test_releases_and_namespaces_cannot_share_managed_storage_roots(self):
        roots = set()
        for release, namespace in [("a", "tests"), ("b", "tests"), ("a", "other")]:
            docs = resources(managed=True, release=release, namespace=namespace)
            env = environment(pod(docs["Job"])["containers"][0])
            roots.add(env["MILVUS_JNI_S3_ROOT_PATH"]["value"])
        self.assertEqual(len(roots), 3)
        long_name = "r" * 53
        docs = resources(managed=True, release=long_name)
        for doc in docs.values():
            self.assertLessEqual(len(doc["metadata"]["name"]), 63)

    def test_private_setup_runs_after_readiness_with_the_exact_test_environment(self):
        setup = {"runner": {"setup": {"command": ["/private/prepare"],
                                       "args": ["--collection", "test-data"]}}}
        spec = pod(resources(managed=True, overrides=setup)["Job"])
        self.assertEqual([c["name"] for c in spec["initContainers"]],
                         ["wait-for-milvus", "prepare-test-data"])
        preparation = spec["initContainers"][1]
        test = spec["containers"][0]
        self.assertEqual(preparation["image"], test["image"])
        self.assertEqual(preparation["env"], test["env"])
        self.assertEqual(preparation["args"], ["--collection", "test-data"])
        self.assertEqual(resources(managed=True)["Job"]["spec"]["backoffLimit"], 0)
        external = pod(resources(overrides=setup)["Job"])
        self.assertEqual([c["name"] for c in external["initContainers"]], ["prepare-test-data"])

    def test_endpoint_ports_are_consistent_for_http_and_https(self):
        for endpoint, ssl, expected in [("s3.example", False, 80),
                                        ("s3.example", True, 443),
                                        ("minio.example:9000", False, 9000)]:
            docs = resources(managed=True, overrides={"objectStorage": {
                "endpoint": endpoint, "useSSL": ssl}})
            config = yaml.safe_load(docs["ConfigMap"]["data"]["user.yaml"])
            self.assertEqual(config["minio"]["port"], expected)
            self.assertEqual(config["minio"]["useSSL"], ssl)

    def test_invalid_managed_or_external_configuration_is_rejected(self):
        for managed, values in [
            (False, {"milvus": {"uri": ""}}),
            (True, {"milvus": {"uri": "http://unrelated:19530"}}),
            (True, {"milvus": {"credentials": {"rootPasswordKey": ""}}}),
            (True, {"objectStorage": {"endpoint": "https://s3.example/path"}}),
            (True, {"objectStorage": {"endpoint": "s3.example:65536"}}),
            (True, {"objectStorage": {"rootPath": "../existing"}}),
            (True, {"job": {"activeDeadlineSeconds": 60}}),
            (True, {"runner": {"setup": {"args": ["orphan-argument"]}}}),
        ]:
            with self.subTest(managed=managed, values=values):
                self.assertNotEqual(render(managed=managed, overrides=values).returncode, 0)

    def test_readiness_command_succeeds_when_healthy_and_fails_at_its_deadline(self):
        init = pod(resources(managed=True)["Job"])["initContainers"][0]
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            date = folder / "date"
            date.write_text('#!/bin/sh\nif [ -f "$CHECK_MARKER" ]; then echo 9999; '
                            'else touch "$CHECK_MARKER"; echo 0; fi\n')
            date.chmod(0o755)
            curl = folder / "curl"
            env = dict(os.environ, PATH=directory + ":" + os.environ["PATH"],
                       CHECK_MARKER=str(folder / "marker"))
            for status in (0, 1):
                curl.write_text(f"#!/bin/sh\nexit {status}\n")
                curl.chmod(0o755)
                (folder / "marker").unlink(missing_ok=True)
                result = subprocess.run(init["command"] + init["args"], env=env,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, status)
                if status:
                    self.assertIn("readiness deadline", result.stderr)


if __name__ == "__main__":
    unittest.main()
