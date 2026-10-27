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

# Build the documentation for each release listed in docs/source/_static/versions.json
#
# The release is checked out into a temporary worktree, writing the result
# to <output>/<version>/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="$(cd "${DOCS_DIR}/.." && pwd)"
OVERLAY_DIR="${SCRIPT_DIR}/release"
MANIFEST="${DOCS_DIR}/source/_static/versions.json"

# Where to fetch release tags from (forks usually do not contain release tags)
UPSTREAM_URL="${DATAFUSION_UPSTREAM_URL:-https://github.com/apache/datafusion.git}"

OUTPUT_DIR="${DOCS_DIR}/build/html/versions"
WORK_DIR=""
VERSIONS=()

usage() {
  cat <<USAGE
Build the documentation for released DataFusion versions.

Usage: $(basename "$0") [options] [VERSION ...]

Builds the documentation for each VERSION from its git tag. With no VERSION,
builds every release listed in ${MANIFEST#"${REPO_DIR}/"}.

Options:
  -o, --output DIR   Write the documentation to DIR/<version>/
                     (default: docs/build/html/versions, so that the versions
                     appear under a site built with docs/build.sh)
  -w, --work DIR     Check the tags out under DIR/<version> instead of a
                     temporary directory
  -h, --help         Show this help message

Environment:
  DATAFUSION_UPSTREAM_URL   Repository to fetch missing release tags from
                            (default: ${UPSTREAM_URL})
  DATAFUSION_DOCS_BASE_PATH Path the site is served under (default: /),
                            see docs/README.md

Example: preview the site with the 55.0.0 docs under versions/55.0.0/
  cd docs
  uv run --package datafusion-docs ./build.sh
  scripts/build_versions.sh 55.0.0
  python3 -m http.server --directory build/html 8000
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    -w|--work)
      WORK_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      VERSIONS+=("$1")
      shift
      ;;
  esac
done

for tool in git uv python3; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "${tool} is required." >&2
    exit 1
  fi
done

# Releases listed in the version picker manifest, newest first.
list_versions() {
  python3 - "${MANIFEST}" <<'PY'
import json
import sys

with open(sys.argv[1]) as f:
    for entry in json.load(f):
        if entry["version"] != "main":
            print(entry["version"])
PY
}

if [[ ${#VERSIONS[@]} -eq 0 ]]; then
  while IFS= read -r version; do
    VERSIONS+=("${version}")
  done < <(list_versions)
fi

if [[ ${#VERSIONS[@]} -eq 0 ]]; then
  echo "No versions to build." >&2
  exit 1
fi

TEMP_WORK_DIR=""
if [[ -z "${WORK_DIR}" ]]; then
  TEMP_WORK_DIR="$(mktemp -d -t datafusion-docs-versions.XXXXXX)"
  WORK_DIR="${TEMP_WORK_DIR}"
fi
mkdir -p "${WORK_DIR}" "${OUTPUT_DIR}"
WORK_DIR="$(cd "${WORK_DIR}" && pwd)"
OUTPUT_DIR="$(cd "${OUTPUT_DIR}" && pwd)"

cleanup() {
  if [[ -n "${TEMP_WORK_DIR}" ]]; then
    for worktree in "${TEMP_WORK_DIR}"/*/; do
      [[ -d "${worktree}" ]] || continue
      git -C "${REPO_DIR}" worktree remove --force "${worktree}" >/dev/null 2>&1 || true
    done
    rm -rf "${TEMP_WORK_DIR}"
    git -C "${REPO_DIR}" worktree prune
  fi
}
trap cleanup EXIT

# Make sure this checkout has the release tag, fetching it from upstream
# otherwise (a shallow CI checkout stays shallow).
fetch_tag() {
  local version="$1"
  if git -C "${REPO_DIR}" rev-parse -q --verify "refs/tags/${version}^{commit}" >/dev/null; then
    return
  fi
  echo "Fetching tag ${version} from ${UPSTREAM_URL}"
  local depth=()
  if [[ "$(git -C "${REPO_DIR}" rev-parse --is-shallow-repository)" == "true" ]]; then
    depth=(--depth 1)
  fi
  git -C "${REPO_DIR}" fetch "${depth[@]}" "${UPSTREAM_URL}" "refs/tags/${version}:refs/tags/${version}"
}

build_version() {
  local version="$1"
  local worktree="${WORK_DIR}/${version}"
  local output="${OUTPUT_DIR}/${version}"

  echo "==> Building documentation for ${version}"
  fetch_tag "${version}"

  if [[ -e "${worktree}" ]]; then
    git -C "${REPO_DIR}" worktree remove --force "${worktree}" >/dev/null 2>&1 || rm -rf "${worktree}"
  fi
  git -C "${REPO_DIR}" worktree add --detach "${worktree}" "refs/tags/${version}"

  # The workspace dependency graph is generated at build time and embedded in
  # the contributor guide (docs/build.sh does the same for the current docs).
  if [[ -x "${worktree}/docs/scripts/generate_dependency_graph.sh" ]]; then
    "${worktree}/docs/scripts/generate_dependency_graph.sh"
  fi

  rm -rf "${output}"
  (
    cd "${DOCS_DIR}"
    DATAFUSION_DOCS_SOURCE="${worktree}/docs/source" \
    DATAFUSION_DOCS_VERSION="${version}" \
      uv run --package datafusion-docs \
        sphinx-build -b html -W -c "${OVERLAY_DIR}" "${worktree}/docs/source" "${output}"
  )
  # Sphinx's incremental build state is not part of the published site.
  rm -f "${output}/.buildinfo"

  git -C "${REPO_DIR}" worktree remove --force "${worktree}"
  echo "==> Wrote ${output}"
}

for version in "${VERSIONS[@]}"; do
  build_version "${version}"
done
