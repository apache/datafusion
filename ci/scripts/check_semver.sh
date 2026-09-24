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

# Compares the public API of the changed crates with a baseline commit, the
# same way the "Detect breaking changes" workflow does, through
# `ci/scripts/changed_crates.sh`. The baseline ref is never fetched.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DEFAULT_BASE_REF="apache/main"
FETCH_HINT="git fetch https://github.com/apache/datafusion.git main:refs/remotes/apache/main"

usage() {
  cat >&2 <<EOF
Usage: $0 [--base-ref REF] [--package NAME]...

Compares the public API of the changed publishable crates with a baseline
commit using cargo-semver-checks.

--base-ref REF  Baseline to compare against. Without it, \$DATAFUSION_SEMVER_BASE_REF
                is used, then the local ref '${DEFAULT_BASE_REF}'.
--package NAME  Check this crate instead of the automatic selection. Repeatable.
-h, --help      Show this message.

The baseline ref is not fetched. Create it with:
  ${FETCH_HINT}
Without that ref the check reports a skip. A ref you name yourself must exist.
EOF
  exit 1
}

BASE_REF=""
PACKAGES=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-ref)
      [[ $# -ge 2 ]] || { echo "[${SCRIPT_NAME}] --base-ref needs a value" >&2; usage; }
      BASE_REF="$2"
      shift
      ;;
    --package)
      [[ $# -ge 2 ]] || { echo "[${SCRIPT_NAME}] --package needs a value" >&2; usage; }
      PACKAGES+=("$2")
      shift
      ;;
    --write|--allow-dirty)
      echo "[${SCRIPT_NAME}] $1 is not supported: an API compatibility finding has no automatic fix." >&2
      usage
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "[${SCRIPT_NAME}] Unknown argument: $1" >&2
      usage
      ;;
  esac
  shift
done

require_tool() {
  local cmd="$1"
  local hint="$2"
  if ! command -v "$cmd" &> /dev/null; then
    echo "[${SCRIPT_NAME}] ${cmd} is required. ${hint}" >&2
    exit 1
  fi
}

require_tool git "Install Git and run this from a DataFusion checkout."
require_tool cargo "Install Rust and Cargo: https://rustup.rs"
require_tool jq "Install jq: https://jqlang.github.io/jq/download/"

cd "${ROOT_DIR}"

# Baseline precedence: --base-ref, then the environment, then the local ref
# that the hosted workflow also uses. Only the last one is a default nobody
# asked for.
BASE_REF_IS_DEFAULT=0
if [[ -z "$BASE_REF" ]]; then
  BASE_REF="${DATAFUSION_SEMVER_BASE_REF:-}"
  if [[ -z "$BASE_REF" ]]; then
    BASE_REF="${DEFAULT_BASE_REF}"
    BASE_REF_IS_DEFAULT=1
  fi
fi

# An unusable baseline leaves nothing to compare against. With the default ref
# that is a setup step the contributor has not taken, so the check reports a
# skip and the lint suite carries on. A ref they named themselves is an error.
unusable_baseline() {
  local reason="$1"
  local hint="$2"
  if [[ $BASE_REF_IS_DEFAULT -eq 0 ]]; then
    echo "[${SCRIPT_NAME}] Baseline '${BASE_REF}' ${reason}" >&2
    echo "[${SCRIPT_NAME}] ${hint}" >&2
    exit 1
  fi
  echo "[${SCRIPT_NAME}] Skipped, no API comparison was made: the default baseline"
  echo "[${SCRIPT_NAME}] '${BASE_REF}' ${reason}"
  echo "[${SCRIPT_NAME}] ${hint}"
  exit 0
}

if ! BASE_SHA="$(git rev-parse --verify --quiet "${BASE_REF}^{commit}")"; then
  unusable_baseline "is not in this repository." \
    "Create the ref CI uses with: ${FETCH_HINT}"
fi

if ! git merge-base "${BASE_SHA}" HEAD > /dev/null 2>&1; then
  unusable_baseline "(${BASE_SHA}) shares no history with HEAD." \
    "Deepen a shallow clone with \`git fetch --unshallow\`, then refresh it: ${FETCH_HINT}"
fi

echo "[${SCRIPT_NAME}] Baseline: ${BASE_REF} (${BASE_SHA})"
echo "[${SCRIPT_NAME}] This ref is never fetched and goes stale. Refresh it with: ${FETCH_HINT}"

publishable_packages() {
  cargo metadata --no-deps --format-version 1 | jq -r '
    .packages[] | select(.publish != []) | .name
  '
}

if [[ ${#PACKAGES[@]} -gt 0 ]]; then
  # An explicit list replaces the automatic selection, so an unknown name must
  # be an error rather than a silently unchecked crate.
  known_packages="$(publishable_packages)"
  invalid=()
  for pkg in "${PACKAGES[@]}"; do
    if ! grep -qxF -- "$pkg" <<<"$known_packages"; then
      invalid+=("$pkg")
    fi
  done
  if [[ ${#invalid[@]} -gt 0 ]]; then
    echo "[${SCRIPT_NAME}] Not publishable workspace crates: ${invalid[*]}" >&2
    exit 1
  fi
else
  # Uncommitted edits count, so a crate whose API changed in the working tree
  # is not silently skipped.
  selection="$("${SCRIPT_DIR}/changed_crates.sh" changed-crates "${BASE_REF}" --include-working-tree)"
  # The helper prints a space-separated list. `read -ra` splits it without
  # expanding globs.
  read -ra PACKAGES <<<"$selection"
fi

if [[ ${#PACKAGES[@]} -eq 0 ]]; then
  echo "[${SCRIPT_NAME}] No publishable crate changed against ${BASE_REF}; nothing to compare."
  echo "[${SCRIPT_NAME}] Selection follows crate directories. Use --package NAME for a crate outside them."
  exit 0
fi

# `datafusion-substrait`, and the crates that reach it, run a build script that
# calls protoc. Both tools are needed only once a crate is actually selected.
require_tool cargo-semver-checks "Install it with: cargo install cargo-semver-checks --locked"
require_tool protoc "Install the Protocol Buffers compiler, for example: brew install protobuf, or apt-get install protobuf-compiler"

echo "[${SCRIPT_NAME}] \`changed_crates.sh semver-check ${BASE_REF} ${PACKAGES[*]}\`"

set +e
"${SCRIPT_DIR}/changed_crates.sh" semver-check "${BASE_REF}" "${PACKAGES[@]}"
STATUS=$?
set -e

if [[ $STATUS -ne 0 ]]; then
  echo "[${SCRIPT_NAME}] cargo-semver-checks exited with ${STATUS}. The output above is either" >&2
  echo "[${SCRIPT_NAME}] a compatibility finding to review or a build error, not a verdict." >&2
fi

exit $STATUS
