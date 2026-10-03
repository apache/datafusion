#!/usr/bin/env bash
#
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

# Runs the default CodeQL query suite for GitHub Actions and writes a SARIF
# report, the same queries the "Analyze Actions" job in the "CodeQL" workflow
# runs. Nothing is uploaded. A zero exit status means the analysis completed,
# not that the repository has no findings.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# The pack default suite, which the hosted analysis also uses.
QUERY_SUITE="codeql/actions-queries:codeql-suites/actions-code-scanning.qls"
# Matches the hosted workflow's `category`.
SARIF_CATEGORY="/language:actions"
REPORT_PARENT="${ROOT_DIR}/target/codeql-actions"
SETUP_DOCS="https://docs.github.com/en/code-security/how-tos/find-and-fix-code-vulnerabilities/scan-from-the-command-line/set-up-codeql-cli"

usage() {
  cat >&2 <<USAGE
Usage: $0

Runs the default CodeQL query suite for GitHub Actions and writes a SARIF
report. There is no write mode, because a security finding needs a reviewed
source change.

Needs the complete CodeQL bundle on PATH, not the CLI-only download, which has
no query packs. The baseline is CodeQL CLI 2.27.0, the default of the
github/codeql-action commit pinned in .github/workflows/codeql.yml. See
${SETUP_DOCS}

Each run writes a new directory under target/codeql-actions, which Git
ignores, and keeps the report and its logs there. A zero exit status means the
analysis completed, not that the repository has no findings.
USAGE
  exit 1
}

# Reject unsupported arguments before any database or report directory exists.
if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      ;;
    *)
      echo "[${SCRIPT_NAME}] unsupported argument: $1" >&2
      usage
      ;;
  esac
fi

if ! command -v codeql &> /dev/null; then
  echo "[${SCRIPT_NAME}] codeql was not found on PATH. Install the complete CodeQL bundle, which ships the Actions extractor and the query packs, and add its codeql directory to PATH: ${SETUP_DOCS}" >&2
  exit 1
fi

if ! CODEQL_VERSION="$(codeql version --format=terse 2>/dev/null)"; then
  echo "[${SCRIPT_NAME}] \`codeql version\` failed. Check the CodeQL installation on PATH: $(command -v codeql)" >&2
  exit 1
fi

if ! command -v python3 &> /dev/null; then
  echo "[${SCRIPT_NAME}] python3 was not found on PATH. It reads the generated SARIF summary. Run the suite through uv, which provides Python: uv run ./dev/rust_lint.sh" >&2
  exit 1
fi

if ! CODEQL_LANGUAGES="$(codeql resolve languages 2>/dev/null)"; then
  echo "[${SCRIPT_NAME}] \`codeql resolve languages\` failed. Check the CodeQL installation on PATH: $(command -v codeql)" >&2
  exit 1
fi

# `grep` runs without `-q` so it reads all of its input, which keeps `pipefail`
# from reporting a SIGPIPE-killed `codeql` as a failure on a match.
if ! printf '%s\n' "${CODEQL_LANGUAGES}" | grep -E '^actions[[:space:]]' > /dev/null; then
  echo "[${SCRIPT_NAME}] this CodeQL installation has no \`actions\` extractor. Install the complete bundle rather than the CLI-only download: ${SETUP_DOCS}" >&2
  exit 1
fi

# Proves the pack and its default suite resolve before a database is created.
if ! codeql resolve queries "${QUERY_SUITE}" > /dev/null 2>&1; then
  echo "[${SCRIPT_NAME}] this CodeQL installation does not resolve ${QUERY_SUITE}. Install the complete bundle, which ships the pack: ${SETUP_DOCS}" >&2
  exit 1
fi

ACTIONS_PACK_VERSION="$(codeql resolve qlpacks --format=json 2>/dev/null | python3 -c '
import json
import os
import sys

try:
    packs = json.load(sys.stdin)
    paths = packs["codeql/actions-queries"]
    print(os.path.basename(paths[0].rstrip("/")))
except Exception:
    print("unknown")
')"

echo "[${SCRIPT_NAME}] CodeQL CLI ${CODEQL_VERSION}"
echo "[${SCRIPT_NAME}] query pack codeql/actions-queries ${ACTIONS_PACK_VERSION}"
echo "[${SCRIPT_NAME}] query suite ${QUERY_SUITE}"

# One report directory per run, so concurrent and repeated runs stay separate.
mkdir -p "${REPORT_PARENT}"
REPORT_DIR="$(mktemp -d "${REPORT_PARENT}/run-XXXXXX")"
REPORT="${REPORT_DIR}/actions.sarif"
CREATE_LOG="${REPORT_DIR}/database-create.log"
ANALYZE_LOG="${REPORT_DIR}/database-analyze.log"

# Only this run's scratch directory is cleaned; the report directory is kept.
SCRATCH_DIR=""
cleanup() {
  if [[ -n "${SCRATCH_DIR}" ]]; then
    rm -rf "${SCRATCH_DIR}"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
SCRATCH_DIR="$(mktemp -d "${TMPDIR:-/tmp}/codeql-actions-db.XXXXXX")"
DATABASE_DIR="${SCRATCH_DIR}/database"

echo "[${SCRIPT_NAME}] extracting the Actions database from ${ROOT_DIR}"
if ! codeql database create "${DATABASE_DIR}" \
  --language=actions \
  --source-root="${ROOT_DIR}" 2>&1 | tee "${CREATE_LOG}"; then
  echo "" >&2
  echo "❌ [${SCRIPT_NAME}] CodeQL extraction failed. Log: ${CREATE_LOG}" >&2
  exit 1
fi

echo "[${SCRIPT_NAME}] analyzing with ${QUERY_SUITE}"
# `--no-download` keeps the analysis on the installed pack, off the network.
if ! codeql database analyze "${DATABASE_DIR}" "${QUERY_SUITE}" \
  --format=sarif-latest \
  --sarif-category="${SARIF_CATEGORY}" \
  --no-download \
  --output="${REPORT}" 2>&1 | tee "${ANALYZE_LOG}"; then
  echo "" >&2
  echo "❌ [${SCRIPT_NAME}] CodeQL analysis failed. Log: ${ANALYZE_LOG}" >&2
  exit 1
fi

# Malformed output is an error, and a missing field is never read as a severity.
SUMMARY_STATUS=0
python3 - "${REPORT}" <<'PYTHON' || SUMMARY_STATUS=$?
import collections
import json
import sys

report_path = sys.argv[1]

try:
    with open(report_path, encoding="utf-8") as handle:
        report = json.load(handle)
except (OSError, ValueError) as error:
    print(f"unreadable SARIF report {report_path}: {error}", file=sys.stderr)
    sys.exit(1)

if not isinstance(report, dict):
    print(f"malformed SARIF report {report_path}: top level is not an object", file=sys.stderr)
    sys.exit(1)

runs = report.get("runs")
if not isinstance(runs, list):
    print(f"malformed SARIF report {report_path}: \"runs\" is missing or not a list", file=sys.stderr)
    sys.exit(1)

findings = []
for run_index, run in enumerate(runs):
    if not isinstance(run, dict):
        print(f"malformed SARIF report {report_path}: run {run_index} is not an object", file=sys.stderr)
        sys.exit(1)
    results = run.get("results", [])
    if not isinstance(results, list):
        print(f"malformed SARIF report {report_path}: run {run_index} has a non-list \"results\"", file=sys.stderr)
        sys.exit(1)
    for result_index, result in enumerate(results):
        if not isinstance(result, dict):
            print(f"malformed SARIF report {report_path}: run {run_index} result {result_index} is not an object", file=sys.stderr)
            sys.exit(1)
        rule = result.get("ruleId")
        if not isinstance(rule, str) or not rule:
            print(f"malformed SARIF report {report_path}: run {run_index} result {result_index} has no \"ruleId\"", file=sys.stderr)
            sys.exit(1)
        location = "location unavailable"
        try:
            physical = result["locations"][0]["physicalLocation"]
            uri = physical["artifactLocation"]["uri"]
            line = physical.get("region", {}).get("startLine")
            location = f"{uri}:{line}" if isinstance(line, int) else uri
        except (KeyError, IndexError, TypeError):
            pass
        findings.append((rule, location))

if not findings:
    print("✅ CodeQL analysis completed with no findings.")
    sys.exit(0)

print(f"⚠️  CodeQL analysis completed with {len(findings)} finding(s):")
print("")
for rule, count in sorted(collections.Counter(rule for rule, _ in findings).items()):
    print(f"  {rule}: {count}")
print("")
for rule, location in findings:
    print(f"  {location}\t{rule}")
PYTHON

if [[ ${SUMMARY_STATUS} -ne 0 ]]; then
  echo "" >&2
  echo "❌ [${SCRIPT_NAME}] could not read the SARIF report. Log: ${ANALYZE_LOG}" >&2
  exit 1
fi

echo ""
echo "[${SCRIPT_NAME}] report: ${REPORT}"
echo "[${SCRIPT_NAME}] a successful run means the analysis completed, not that the repository is free of findings."
echo "[${SCRIPT_NAME}] the report and its logs stay on disk. Remove them with: rm -rf ${REPORT_DIR}"
