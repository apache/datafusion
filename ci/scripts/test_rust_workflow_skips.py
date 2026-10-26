#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Regression tests for the Rust workflow skip policies."""

import unittest
from pathlib import Path

import yaml

from check_asf_yaml_status_checks import get_required_checks
from check_rust_workflow_skips import check_skip_conditions

ROOT = Path(__file__).resolve().parents[2]


class RustWorkflowSkipsTest(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.safe_load((ROOT / ".github/workflows/rust.yml").read_text())
        self.required_checks = get_required_checks(ROOT / ".asf.yaml")

    def errors(self):
        return check_skip_conditions(self.workflow, self.required_checks)

    def test_current_workflow(self):
        self.assertEqual(self.errors(), [])

    def test_missing_skip_guard(self):
        del self.workflow["jobs"]["linux-test"]["if"]
        self.assertTrue(
            any("job `linux-test` must carry exactly" in error for error in self.errors())
        )

    def test_unskipped_job_needs_skipped_job(self):
        self.workflow["jobs"]["check-fmt"]["needs"] = "linux-test"
        self.assertIn(
            "  - job `check-fmt` is not in FORK_PR_SKIPPED_JOBS but needs "
            "['linux-test']; GitHub would skip it with them",
            self.errors(),
        )

    def test_missing_artifact_upload_guard(self):
        upload = next(
            step
            for step in self.workflow["jobs"]["linux-build-lib"]["steps"]
            if step.get("uses", "").startswith("actions/upload-artifact")
            and step.get("with", {}).get("name") == "cargo-check"
        )
        del upload["if"]
        self.assertTrue(
            any("`cargo-check` upload step" in error for error in self.errors())
        )


if __name__ == "__main__":
    unittest.main()
