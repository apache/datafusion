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

# Fails if any file committed between a base ref and a head ref is larger than
# the size limit, the same way the "Large files PR check" GitHub workflow does.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
ROOT_DIR="$(git rev-parse --show-toplevel)"

# 1.5 MB ought to be enough for anybody.
# TODO in case we may want to consciously commit a bigger file to the repo
# without using Git LFS we may disable the check e.g. with a label
DEFAULT_MAX_FILE_SIZE_BYTES=1572864

BASE_REF=""
HEAD_REF="HEAD"
MAX_FILE_SIZE_BYTES="${DEFAULT_MAX_FILE_SIZE_BYTES}"

usage() {
  cat >&2 <<USAGE
Usage: $SCRIPT_NAME [--base <ref>] [--head <ref>] [--max-bytes <n>]
Fails if any file committed between <base> and <head> is larger than <n> bytes.
--base <ref>     Start of the commit range, exclusive. Defaults to the merge base of <head> and main on the
                 remote that points at apache/datafusion, or on origin when there is no such remote.
--head <ref>     End of the commit range, inclusive. Defaults to HEAD.
--max-bytes <n>  Size limit in bytes. Defaults to ${DEFAULT_MAX_FILE_SIZE_BYTES} (1.5 MB).
USAGE
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base)
      [[ $# -ge 2 ]] || usage
      BASE_REF="$2"
      shift
      ;;
    --head)
      [[ $# -ge 2 ]] || usage
      HEAD_REF="$2"
      shift
      ;;
    --max-bytes)
      [[ $# -ge 2 ]] || usage
      MAX_FILE_SIZE_BYTES="$2"
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
  shift
done

# A leading zero is rejected too: bash would read the value as octal.
if ! [[ "${MAX_FILE_SIZE_BYTES}" =~ ^(0|[1-9][0-9]*)$ ]]; then
  echo "[${SCRIPT_NAME}] --max-bytes must be a whole number of bytes, got \"${MAX_FILE_SIZE_BYTES}\"" >&2
  exit 1
fi

cd "${ROOT_DIR}"

# The remote that points at apache/datafusion, or origin when there is no such remote.
upstream_remote() {
  local remote
  for remote in $(git remote); do
    if [[ "$(git remote get-url "${remote}")" =~ github\.com[:/]apache/datafusion(\.git)?/?$ ]]; then
      echo "${remote}"
      return
    fi
  done
  echo "origin"
}

if ! git rev-parse --verify --quiet "${HEAD_REF}^{commit}" > /dev/null; then
  echo "[${SCRIPT_NAME}] ${HEAD_REF} is not a commit" >&2
  exit 1
fi

if [[ -z "${BASE_REF}" ]]; then
  upstream_main="$(upstream_remote)/main"
  if ! git rev-parse --verify --quiet "${upstream_main}^{commit}" > /dev/null; then
    echo "[${SCRIPT_NAME}] ${upstream_main} is not available; fetch it or pass --base <ref> to choose the start of the range" >&2
    exit 1
  fi
  BASE_REF="$(git merge-base "${HEAD_REF}" "${upstream_main}")"
elif ! git rev-parse --verify --quiet "${BASE_REF}^{commit}" > /dev/null; then
  echo "[${SCRIPT_NAME}] ${BASE_REF} is not a commit" >&2
  exit 1
fi

echo "[${SCRIPT_NAME}] Checking files committed in ${BASE_REF}..${HEAD_REF} against the ${MAX_FILE_SIZE_BYTES} byte limit"

# Listing the objects into a variable, rather than piping them into the loop, makes a failing
# git command fail the check instead of leaving the loop with nothing to read.
objects="$(
  git rev-list --objects "${BASE_REF}..${HEAD_REF}" \
    | git cat-file --batch-check='%(objectname) %(objecttype) %(objectsize) %(rest)'
)"

exit_code=0
# Only blobs are files. Commits and trees are listed too, and are skipped.
while read -r id type size path; do
  if [[ "${type}" == "missing" ]]; then
    echo "[${SCRIPT_NAME}] Object ${id} is not in this clone, so its size cannot be checked. Fetch the full history and retry." >&2
    exit 1
  fi
  if [[ "${type}" == "blob" && "${size}" -gt "${MAX_FILE_SIZE_BYTES}" ]]; then
    exit_code=1
    echo "Object ${id} [${path}] has size ${size}, exceeding ${MAX_FILE_SIZE_BYTES} limit." >&2
    if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
      echo "::error file=${path}::File ${path} has size ${size}, exceeding ${MAX_FILE_SIZE_BYTES} limit."
    fi
  fi
done <<< "${objects}"

exit "${exit_code}"
