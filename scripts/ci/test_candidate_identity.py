import unittest

import candidate_identity


class CandidateIdentityTest(unittest.TestCase):
    def test_coordinate_cannot_be_reused_by_another_run_or_attempt(self):
        environment = {"GITHUB_RUN_ID": "12345", "GITHUB_RUN_ATTEMPT": "1", "GITHUB_REF_TYPE": "branch"}
        candidate_identity.check("3.0.0-rc.12345.1", environment)
        for changes in ({"GITHUB_RUN_ID": "12346"}, {"GITHUB_RUN_ATTEMPT": "2"}, {"GITHUB_RUN_ID": ""}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                candidate_identity.check("3.0.0-rc.12345.1", dict(environment, **changes))

    def test_release_coordinate_is_bound_to_a_pushed_tag(self):
        environment = {"GITHUB_REF_TYPE": "tag", "GITHUB_REF_NAME": "v3.0.0", "GITHUB_EVENT_NAME": "push"}
        candidate_identity.check("3.0.0", environment)
        for changes in ({"GITHUB_REF_NAME": "v3.0.1"}, {"GITHUB_EVENT_NAME": "workflow_dispatch"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                candidate_identity.check("3.0.0", dict(environment, **changes))
