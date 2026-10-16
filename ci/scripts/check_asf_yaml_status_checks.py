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

Also validate the post-merge conditions in rust.yml: exactly the jobs
in POST_MERGE_SKIPPED_JOBS carry the condition that skips them on
pushes to upstream `main`, and the Cargo check artifact steps are
guarded the same way.
"""

import glob
import os
import sys

import yaml

# Commits reach upstream `main` through the merge queue, which already ran
# rust.yml at the same SHA. On a push to upstream `main`, rust.yml skips the
# jobs below and keeps only the jobs that save caches, publish coverage, or run
# the non-required FFI check. A job is skipped only when it is listed here and
# carries exactly POST_MERGE_SKIP_CONDITION, so a new job runs by default and
# changing the set is a deliberate edit to both places.
RUST_WORKFLOW = "rust.yml"
POST_MERGE_PUSH = (
    "github.event_name == 'push'"
    " && github.ref == 'refs/heads/main'"
    " && github.repository == 'apache/datafusion'"
)
POST_MERGE_SKIP_CONDITION = "${{ !(" + POST_MERGE_PUSH + ") }}"
POST_MERGE_SKIPPED_JOBS = frozenset(
    {
        "cargo-toml-formatting-checks",
        "check-fmt",
        "check-workflow-tool-installs",
        "config-docs-check",
        "examples-docs-check",
        "linux-cargo-check-datafusion",
        "linux-cargo-check-datafusion-spark",
        "linux-datafusion-common-features",
        "linux-datafusion-proto-features",
        "linux-datafusion-substrait-features",
        "linux-rustdoc",
        "linux-test-datafusion-cli",
        "linux-test-doc",
        "linux-wasm-pack",
        "msrv",
        "sqllogictest-postgres",
        "sqllogictest-substrait",
        "vendor",
        "verify-benchmark-results",
    }
)
# `linux-build-lib` uploads its Cargo check artifacts for two skipped jobs, so
# the archive and upload steps skip on the same pushes.
ARTIFACT_JOB = "linux-build-lib"
ARTIFACT_ARCHIVE_STEP = "archive-check"
ARTIFACT_NAME = "cargo-check"
ARTIFACT_UPLOAD_CONDITION = (
    "${{ steps." + ARTIFACT_ARCHIVE_STEP + ".outcome == 'success'"
    " && !(" + POST_MERGE_PUSH + ") }}"
)


def load_workflow(path):
    with open(path) as f:
        return yaml.safe_load(f)


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
        workflow = load_workflow(workflow_file)

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


def iter_steps(job_config):
    """Yield the steps of a job, flattening `parallel:` groups."""
    for step in job_config.get("steps") or []:
        if not isinstance(step, dict):
            continue
        if "parallel" in step:
            yield from (s for s in step["parallel"] or [] if isinstance(s, dict))
        else:
            yield step


def uses_action(step, action):
    return str(step.get("uses", "")).startswith(action)


def check_post_merge_conditions(workflow):
    """Return the problems with rust.yml's post-merge job and artifact conditions."""
    errors = []
    jobs = {
        key: config
        for key, config in (workflow.get("jobs") or {}).items()
        if isinstance(config, dict)
    }

    for job in sorted(POST_MERGE_SKIPPED_JOBS - set(jobs)):
        errors.append(
            f"  - job `{job}` is listed in POST_MERGE_SKIPPED_JOBS "
            f"but does not exist in {RUST_WORKFLOW}"
        )
    for job, config in sorted(jobs.items()):
        condition = config.get("if")
        if job in POST_MERGE_SKIPPED_JOBS:
            if condition != POST_MERGE_SKIP_CONDITION:
                errors.append(
                    f"  - job `{job}` must carry exactly "
                    f"`if: {POST_MERGE_SKIP_CONDITION}`, found {condition!r}"
                )
        elif condition is not None and any(
            token in str(condition)
            for token in ("github.event_name", "github.ref", "refs/heads/")
        ):
            errors.append(
                f"  - job `{job}` is not listed in POST_MERGE_SKIPPED_JOBS "
                f"but carries an event condition: {condition!r}"
            )

    build = jobs.get(ARTIFACT_JOB)
    if build is None:
        errors.append(f"  - job `{ARTIFACT_JOB}` does not exist in {RUST_WORKFLOW}")
    else:
        steps = list(iter_steps(build))
        archive = [s for s in steps if s.get("id") == ARTIFACT_ARCHIVE_STEP]
        uploads = [s for s in steps if uses_action(s, "actions/upload-artifact")]
        if len(archive) != 1 or archive[0].get("if") != POST_MERGE_SKIP_CONDITION:
            found = [s.get("if") for s in archive]
            errors.append(
                f"  - the step `id: {ARTIFACT_ARCHIVE_STEP}` in `{ARTIFACT_JOB}` "
                f"must exist once and carry exactly "
                f"`if: {POST_MERGE_SKIP_CONDITION}`, found {found!r}"
            )
        if len(uploads) != 1 or uploads[0].get("if") != ARTIFACT_UPLOAD_CONDITION:
            found = [s.get("if") for s in uploads]
            errors.append(
                f"  - the upload-artifact step in `{ARTIFACT_JOB}` must exist "
                f"once and carry exactly `if: {ARTIFACT_UPLOAD_CONDITION}`, "
                f"found {found!r}"
            )

    for job, config in sorted(jobs.items()):
        downloads = [
            s
            for s in iter_steps(config)
            if uses_action(s, "actions/download-artifact")
            and (s.get("with") or {}).get("name") == ARTIFACT_NAME
        ]
        if downloads and job not in POST_MERGE_SKIPPED_JOBS:
            errors.append(
                f"  - job `{job}` downloads the `{ARTIFACT_NAME}` artifact, which "
                f"is not uploaded on pushes to main, so it must be listed in "
                f"POST_MERGE_SKIPPED_JOBS"
            )
    return errors


def main():
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    asf_yaml = os.path.join(repo_root, ".asf.yaml")
    workflows_dir = os.path.join(repo_root, ".github", "workflows")

    required_checks = get_required_checks(asf_yaml)
    if not required_checks:
        print("No required_status_checks found in .asf.yaml — nothing to validate.")

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

    post_merge_errors = check_post_merge_conditions(
        load_workflow(os.path.join(workflows_dir, RUST_WORKFLOW))
    )

    if errors:
        print("ERROR: Problems found with required_status_checks in .asf.yaml:\n")
        print("\n".join(errors))
        print()
        print("Available job names across all workflows:")
        for name in sorted(jobs):
            print(f"  - {name}")
    if post_merge_errors:
        print(
            f"ERROR: Problems found with post-merge conditions in {RUST_WORKFLOW}:\n"
        )
        print("\n".join(post_merge_errors))
        print()
    if errors or post_merge_errors:
        sys.exit(1)

    if required_checks:
        print(
            f"OK: All {len(required_checks)} required_status_checks "
            "match existing GitHub Actions jobs."
        )
    print(
        f"OK: {RUST_WORKFLOW} skips exactly {len(POST_MERGE_SKIPPED_JOBS)} "
        "jobs on pushes to main."
    )


if __name__ == "__main__":
    main()
