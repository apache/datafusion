#!/usr/bin/env bash
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

# Prints the `svn delete` commands that clean up the Apache distribution SVN
# server after a release has been published. It only lists the SVN directories
# and never deletes anything. Check the printed commands and then run them.
#
# For the released version it prints deletes for
#   - the release candidates of that version and older versions in the `dev` area
#   - the releases older than the last major release (<major>.0.0) in the
#     `release` area. The last major release and the releases after it are kept.
#
# Usage: ./dev/release/print-svn-deletes.sh <version>
# Example: ./dev/release/print-svn-deletes.sh 55.2.0
#
# DIST_URL can point to another SVN repository, for example to test the script.

set -euo pipefail

usage() {
  echo "Usage: $0 <version>, for example: $0 55.2.0" >&2
  exit 1
}

# Turns X.Y.Z into a number that can be compared, for example 55.2.0 -> 55002000
version_number() {
  local maj min pat
  IFS=. read -r maj min pat <<<"$1"
  echo $((10#$maj * 1000000 + 10#$min * 1000 + 10#$pat))
}

# Prints an svn delete command with one URL per line
print_delete() {
  local message=$1 url
  shift
  printf 'svn delete -m "%s"' "$message"
  for url in "$@"; do
    printf ' \\\n  %s' "$url"
  done
  printf '\n'
}

version_re='^([0-9]+)\.[0-9]+\.[0-9]+$'
rc_re='^apache-datafusion-([0-9]+\.[0-9]+\.[0-9]+)-rc[0-9]+/$'
release_re='^datafusion-([0-9]+)\.[0-9]+\.[0-9]+/$'

[ $# -eq 1 ] && [[ $1 =~ $version_re ]] || usage
version=$1
major=${BASH_REMATCH[1]}
version_num=$(version_number "$version")

dist_url=${DIST_URL:-https://dist.apache.org/repos/dist}
dev_url=$dist_url/dev/datafusion
release_url=$dist_url/release/datafusion

dev_listing=$(svn ls "$dev_url")
release_listing=$(svn ls "$release_url")

# Old files are deleted after the new release is published
if ! grep -qxF "datafusion-$version/" <<<"$release_listing"; then
  echo "datafusion-$version/ is not in $release_url yet, run release-tarball.sh first" >&2
  exit 1
fi

rc_deletes=()
while read -r name; do
  if [[ $name =~ $rc_re ]] && [ "$(version_number "${BASH_REMATCH[1]}")" -le "$version_num" ]; then
    rc_deletes+=("$dev_url/${name%/}")
  fi
done <<<"$dev_listing"

release_deletes=()
kept=()
while read -r name; do
  if [[ $name =~ $release_re ]]; then
    if [ "${BASH_REMATCH[1]}" -lt "$major" ]; then
      release_deletes+=("$release_url/${name%/}")
    else
      kept+=("${name%/}")
    fi
  fi
done <<<"$release_listing"

echo "# Released version: $version"
echo
echo "# dev: release candidates of $version and older"
if [ ${#rc_deletes[@]} -gt 0 ]; then
  print_delete "delete old DataFusion RCs" "${rc_deletes[@]}"
else
  echo "# nothing to delete"
fi
echo
echo "# release: keep $major.0.0 and newer, delete older releases"
if [ ${#kept[@]} -gt 0 ]; then
  echo "# keeping: ${kept[*]}"
fi
if [ ${#release_deletes[@]} -gt 0 ]; then
  print_delete "delete old DataFusion releases" "${release_deletes[@]}"
else
  echo "# nothing to delete"
fi
