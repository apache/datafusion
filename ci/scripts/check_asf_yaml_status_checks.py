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

"""
Validate that every entry in .asf.yaml required_status_checks
matches an actual GitHub Actions job name, and that the workflow
is not filtered by paths/paths-ignore (which would prevent the
check from running on some PRs, blocking merges).

A typo or stale entry in required_status_checks will block all
merges for the project, so this check catches that early.
"""

import glob
import os
import sys

import yaml


def get_required_checks(asf_yaml_path):
    """Extract all required_status_checks contexts from .asf.yaml."""
    with open(asf_yaml_path) as f:
        config = yaml.safe_load(f)

    checks = {}  # context -> list of branches requiring it
    branches = config.get("github", {}).get("protected_branches", {})
    for branch, settings in branches.items():
        contexts = (
            settings.get("required_status_checks", {}).get("contexts", [])
        )
        for ctx in contexts:
            checks.setdefault(ctx, []).append(branch)

    return checks


def get_workflow_jobs(workflows_dir):
    """Collect all jobs with their metadata from GitHub Actions workflow files.

    Returns a dict mapping job identifier (name or key) to a list of
    (workflow_file, has_path_filters) tuples.
    """
    jobs = {}  # identifier -> [(workflow_file, has_path_filters)]
    for workflow_file in sorted(glob.glob(os.path.join(workflows_dir, "*.yml"))):
        with open(workflow_file) as f:
            workflow = yaml.safe_load(f)

        if not workflow or "jobs" not in workflow:
            continue

        # Check if pull_request trigger has path filters
        on = workflow.get(True, workflow.get("on", {}))  # yaml parses `on:` as True
        pr_trigger = on.get("pull_request", {}) if isinstance(on, dict) else {}
        has_path_filters = bool(
            isinstance(pr_trigger, dict)
            and (pr_trigger.get("paths") or pr_trigger.get("paths-ignore"))
        )

        basename = os.path.basename(workflow_file)
        for job_key, job_config in workflow.get("jobs", {}).items():
            if not isinstance(job_config, dict):
                continue
            job_name = job_config.get("name", job_key)
            info = (basename, has_path_filters)
            jobs.setdefault(job_name, []).append(info)
            if job_key != job_name:
                jobs.setdefault(job_key, []).append(info)

    return jobs


def main():
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    asf_yaml = os.path.join(repo_root, ".asf.yaml")
    workflows_dir = os.path.join(repo_root, ".github", "workflows")

    required_checks = get_required_checks(asf_yaml)
    if not required_checks:
        print("No required_status_checks found in .asf.yaml — nothing to validate.")
        return

    jobs = get_workflow_jobs(workflows_dir)
    errors = []

    for ctx in sorted(required_checks):
        branches = ", ".join(sorted(required_checks[ctx]))
        if ctx not in jobs:
            errors.append(
                f'  - "{ctx}" (branch: {branches}): '
                f"not found in any GitHub Actions workflow"
            )
            continue

        # Check if ALL workflows providing this job have path filters
        # (if at least one doesn't, the check will still run)
        filtered_workflows = [
            wf for wf, has_filter in jobs[ctx] if has_filter
        ]
        unfiltered_workflows = [
            wf for wf, has_filter in jobs[ctx] if not has_filter
        ]
        if filtered_workflows and not unfiltered_workflows:
            wf_list = ", ".join(filtered_workflows)
            errors.append(
                f'  - "{ctx}" (branch: {branches}): '
                f"workflow {wf_list} uses paths/paths-ignore filters on "
                f"pull_request, so this check won't run for some PRs "
                f"and will block merging"
            )

    if errors:
        print("ERROR: Problems found with required_status_checks in .asf.yaml:\n")
        print("\n".join(errors))
        print()
        print("Available job names across all workflows:")
        for name in sorted(jobs):
            print(f"  - {name}")
        sys.exit(1)

    print(
        f"OK: All {len(required_checks)} required_status_checks "
        "match existing GitHub Actions jobs."
    )


if __name__ == "__main__":
    main()
