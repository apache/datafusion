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
Validate the job-level skip conditions in rust.yml.

rust.yml skips jobs on two events. Each skip set below pairs a clause with
the jobs that carry it: a job's `if:` is exactly `!(clause)` for every set
it belongs to, joined with ` && ` in the order of SKIP_POLICIES, and a job
in no set carries no job-level `if:`. A new job therefore runs everywhere
by default, and changing a set is a deliberate edit to both files.

The Cargo check artifact steps in `linux-build-lib` and the jobs that
download the artifact must follow the same sets.
"""

import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from check_asf_yaml_status_checks import get_required_checks  # noqa: E402

RUST_WORKFLOW = "rust.yml"

# Skipped on a push to upstream main; see "Pushes to main" in rust.yml.
POST_MERGE_PUSH = (
    "github.event_name == 'push'"
    " && github.ref == 'refs/heads/main'"
    " && github.repository == 'apache/datafusion'"
)
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

# Skipped on a pull request from a fork; see "Pull requests from forks" in
# rust.yml and fork_ci_status.yml.
FORK_PULL_REQUEST = (
    "github.event_name == 'pull_request'"
    " && github.event.pull_request.head.repo.fork"
    " && github.base_ref == 'main'"
)
FORK_PR_SKIPPED_JOBS = frozenset(
    {
        "cargo-toml-formatting-checks",
        "clippy",
        "config-docs-check",
        "examples-docs-check",
        "linux-build-lib",
        "linux-cargo-check-datafusion",
        "linux-cargo-check-datafusion-functions",
        "linux-cargo-check-datafusion-spark",
        "linux-datafusion-common-features",
        "linux-datafusion-ffi-features",
        "linux-datafusion-proto-features",
        "linux-datafusion-substrait-features",
        "linux-rustdoc",
        "linux-test",
        "linux-test-datafusion-cli",
        "linux-test-doc",
        "linux-test-example",
        "linux-wasm-pack",
        "macos-aarch64",
        "msrv",
        "sqllogictest-postgres",
        "sqllogictest-substrait",
        "vendor",
        "verify-benchmark-results",
    }
)

SKIP_POLICIES = (
    ("POST_MERGE_SKIPPED_JOBS", POST_MERGE_SKIPPED_JOBS, POST_MERGE_PUSH),
    ("FORK_PR_SKIPPED_JOBS", FORK_PR_SKIPPED_JOBS, FORK_PULL_REQUEST),
)


def expected_condition(job):
    """The exact job-level `if:` for a job, or None when it must carry none."""
    clauses = [clause for _, jobs, clause in SKIP_POLICIES if job in jobs]
    if not clauses:
        return None
    return "${{ " + " && ".join(f"!({clause})" for clause in clauses) + " }}"


# `linux-build-lib` uploads Cargo check artifacts for two consumers. Its steps
# skip on a push to main and the whole job skips on a fork PR.
ARTIFACT_JOB = "linux-build-lib"
ARTIFACT_ARCHIVE_STEP = "archive-check"
ARTIFACT_NAME = "cargo-check"
ARTIFACT_ARCHIVE_CONDITION = "${{ !(" + POST_MERGE_PUSH + ") }}"
ARTIFACT_UPLOAD_CONDITION = (
    "${{ steps." + ARTIFACT_ARCHIVE_STEP + ".outcome == 'success'"
    " && !(" + POST_MERGE_PUSH + ") }}"
)


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


def job_needs(config):
    needs = config.get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def check_skip_conditions(workflow, required_checks):
    """Return the problems with rust.yml's skip conditions and artifact steps."""
    errors = []
    jobs = {
        key: config
        for key, config in (workflow.get("jobs") or {}).items()
        if isinstance(config, dict)
    }
    listed = frozenset().union(*(members for _, members, _ in SKIP_POLICIES))

    for job in sorted(listed - set(jobs)):
        errors.append(
            f"  - job `{job}` is listed in a skip set but does not exist "
            f"in {RUST_WORKFLOW}"
        )
    for job, config in sorted(jobs.items()):
        expected = expected_condition(job)
        found = config.get("if")
        if found == expected:
            continue
        if expected is None:
            errors.append(
                f"  - job `{job}` is in no skip set, so it must carry no "
                f"job-level `if:`; found {found!r}. List the job in a skip "
                f"set or extend this check."
            )
        else:
            errors.append(
                f"  - job `{job}` must carry exactly `if: {expected}`, "
                f"found {found!r}"
            )

    for set_name, members, _ in SKIP_POLICIES:
        for job, config in sorted(jobs.items()):
            if job in members:
                continue
            blocked = sorted(set(job_needs(config)) & members)
            if blocked:
                errors.append(
                    f"  - job `{job}` is not in {set_name} but needs "
                    f"{blocked}; GitHub would skip it with them"
                )
        # Only the merge queue gates a job that skips after the merge. A job
        # that skips on a fork PR still runs on the fork, in the queue, and on
        # main.
        if required_checks and set_name == "POST_MERGE_SKIPPED_JOBS":
            for job in sorted(members & set(jobs)):
                name = jobs[job].get("name", job)
                if name not in required_checks:
                    errors.append(
                        f"  - job `{job}` ({name!r}) is in {set_name} but is "
                        f"not a required status check in .asf.yaml, so no "
                        f"run would gate it before it lands on main"
                    )

    build = jobs.get(ARTIFACT_JOB)
    if build is None:
        errors.append(f"  - job `{ARTIFACT_JOB}` does not exist in {RUST_WORKFLOW}")
    else:
        steps = list(iter_steps(build))
        archive = [s for s in steps if s.get("id") == ARTIFACT_ARCHIVE_STEP]
        uploads = [
            s
            for s in steps
            if uses_action(s, "actions/upload-artifact")
            and (s.get("with") or {}).get("name") == ARTIFACT_NAME
        ]
        if len(archive) != 1 or archive[0].get("if") != ARTIFACT_ARCHIVE_CONDITION:
            found = [s.get("if") for s in archive]
            errors.append(
                f"  - the step `id: {ARTIFACT_ARCHIVE_STEP}` in `{ARTIFACT_JOB}` "
                f"must exist once and carry exactly "
                f"`if: {ARTIFACT_ARCHIVE_CONDITION}`, found {found!r}"
            )
        if len(uploads) != 1 or uploads[0].get("if") != ARTIFACT_UPLOAD_CONDITION:
            found = [s.get("if") for s in uploads]
            errors.append(
                f"  - the `{ARTIFACT_NAME}` upload step in `{ARTIFACT_JOB}` must "
                f"exist once and carry exactly `if: {ARTIFACT_UPLOAD_CONDITION}`, "
                f"found {found!r}"
            )

    # The artifact is missing on both events, so every consumer must skip on both.
    consumer_sets = [
        (name, members)
        for name, members, _ in SKIP_POLICIES
        if name == "POST_MERGE_SKIPPED_JOBS" or ARTIFACT_JOB in members
    ]
    for job, config in sorted(jobs.items()):
        downloads = [
            s
            for s in iter_steps(config)
            if uses_action(s, "actions/download-artifact")
            and (s.get("with") or {}).get("name") == ARTIFACT_NAME
        ]
        if not downloads:
            continue
        for set_name, members in consumer_sets:
            if job not in members:
                errors.append(
                    f"  - job `{job}` downloads the `{ARTIFACT_NAME}` artifact "
                    f"but is not in {set_name}, where its producer skips"
                )
    return errors


def main():
    repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    with open(os.path.join(repo_root, ".github", "workflows", RUST_WORKFLOW)) as f:
        workflow = yaml.safe_load(f)
    required_checks = get_required_checks(os.path.join(repo_root, ".asf.yaml"))

    errors = check_skip_conditions(workflow, required_checks)
    if errors:
        print(f"ERROR: Problems found with skip conditions in {RUST_WORKFLOW}:\n")
        print("\n".join(errors))
        print()
        sys.exit(1)

    print(
        f"OK: {RUST_WORKFLOW} skips exactly {len(POST_MERGE_SKIPPED_JOBS)} jobs "
        f"on pushes to main and {len(FORK_PR_SKIPPED_JOBS)} jobs on pull "
        "requests from forks."
    )


if __name__ == "__main__":
    main()
