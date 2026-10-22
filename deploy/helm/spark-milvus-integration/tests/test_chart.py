"""Render the public fixed-input read contract without a Kubernetes cluster."""

from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


CHART = Path(__file__).resolve().parents[1]


def render(release="test-a", namespace="tests", overrides=None):
    command = ["helm", "template", release, str(CHART), "--namespace", namespace,
               "--values", str(CHART / "ci-values.read.example.yaml")]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as values:
        yaml.safe_dump(overrides or {}, values)
        values.flush()
        return subprocess.run(command + ["--values", values.name], text=True,
                              capture_output=True)


def resources(**kwargs):
    result = render(**kwargs)
    if result.returncode:
        raise AssertionError(result.stderr)
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def pod(resource):
    return resource["spec"]["template"]["spec"]


def environment(container):
    return {entry["name"]: entry for entry in container["env"]}


class ChartTest(unittest.TestCase):
    def test_fixed_reader_has_only_two_input_identities_and_no_service_configuration(self):
        docs = resources()
        self.assertEqual([doc["kind"] for doc in docs], ["Job"])
        spec = pod(docs[0])
        self.assertNotIn("initContainers", spec)
        self.assertEqual(len(spec["containers"]), 1)
        env = environment(spec["containers"][0])
        self.assertEqual(set(env), {"CI_RUN_ID", "MILVUS_READ_IT_OPTIONS", "MILVUS_READ_IT_OPTIONS_B"})
        self.assertEqual(env["MILVUS_READ_IT_OPTIONS"]["value"], "/etc/milvus-read/a.json")
        self.assertEqual(env["MILVUS_READ_IT_OPTIONS_B"]["value"], "/etc/milvus-read/b.json")
        self.assertTrue(spec["containers"][0]["volumeMounts"][0]["readOnly"])
        self.assertEqual(spec["volumes"][0]["secret"]["secretName"], "v3-read-options")

    def test_secret_is_external_and_values_cannot_embed_credentials(self):
        docs = resources(overrides={"readOptions": {"existingSecret": "different-identities"}})
        self.assertEqual([doc["kind"] for doc in docs], ["Job"])
        self.assertEqual(pod(docs[0])["volumes"][0]["secret"]["secretName"], "different-identities")
        for values in ({"readOptions": {"token": "must-not-appear"}},
                       {"readOptions": {"a.json": "must-not-appear"}}):
            with self.subTest(values=values):
                self.assertNotEqual(render(overrides=values).returncode, 0)

    def test_input_identity_and_immutable_image_are_required(self):
        for values in ({"readOptions": {"existingSecret": ""}},
                       {"readOptions": {"existingSecret": "../invalid"}},
                       {"image": {"repository": ""}},
                       {"image": {"digest": "latest"}},
                       {"runner": {"command": []}}):
            with self.subTest(values=values):
                self.assertNotEqual(render(overrides=values).returncode, 0)

    def test_security_and_single_attempt_deadline_remain_explicit(self):
        job = resources()[0]
        spec = pod(job)
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        self.assertEqual(spec["restartPolicy"], "Never")
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertTrue(spec["securityContext"]["runAsNonRoot"])
        self.assertFalse(spec["containers"][0]["securityContext"]["allowPrivilegeEscalation"])
        self.assertEqual(spec["containers"][0]["securityContext"]["capabilities"]["drop"], ["ALL"])
        self.assertIn("@sha256:", spec["containers"][0]["image"])
        self.assertNotEqual(render(overrides={"job": {"activeDeadlineSeconds": 0}}).returncode, 0)

    def test_releases_preserve_distinct_ownership_labels_and_bounded_names(self):
        names = set()
        for release in ("a", "b", "r" * 53):
            job = resources(release=release)[0]
            name = job["metadata"]["name"]
            names.add(name)
            self.assertLessEqual(len(name), 63)
            self.assertEqual(job["metadata"]["labels"]["app.kubernetes.io/instance"], release)
            self.assertEqual(job["spec"]["template"]["metadata"]["labels"]
                             ["app.kubernetes.io/instance"], release)
        self.assertEqual(len(names), 3)


if __name__ == "__main__":
    unittest.main()
