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

set -ue

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <version> <github-user> <github-token> <homebrew-default-branch-name>"
  exit 1
fi

version=$1
github_user=$2
github_token=$3
# Prepare for possible renaming of the default branch on Homebrew
homebrew_default_branch_name=$4

# Git parallel fetch
if sysctl -n hw.ncpu 2>/dev/null; then # macOS
  num_processing_units=$(sysctl -n hw.ncpu)
elif [ -x "$(command -v nproc)" ]; then # Linux
  num_processing_units=$(nproc)
else # Fallback
  num_processing_units=1
fi

url="https://www.apache.org/dyn/closer.lua?path=datafusion/datafusion-${version}/apache-datafusion-${version}.tar.gz"
sha256="$(curl https://dist.apache.org/repos/dist/release/datafusion/datafusion-${version}/apache-datafusion-${version}.tar.gz.sha256 | cut -d' ' -f1)"

pushd "$(brew --repository homebrew/core)"

if ! git remote | grep -q --fixed-strings ${github_user}; then
  echo "Setting ''${github_user}' remote"
  git remote add ${github_user} git@github.com:${github_user}/homebrew-core.git
fi

echo "Updating working copy"
git fetch --all --prune --tags --force -j$num_processing_units

branch=apache-datafusion-${version}
echo "Creating branch: ${branch}"
git branch -D ${branch} || :
git checkout -b ${branch} origin/master

echo "Updating datafusion formulae"
brew bump-formula-pr \
     --commit \
     --no-audit \
     --sha256="${sha256}" \
     --url="${url}" \
     --verbose \
     --write-only \
     datafusion

echo "Testing datafusion formulae"
brew uninstall datafusion || :
brew install --build-from-source datafusion
brew test datafusion
brew audit --strict datafusion

git push -u $github_user ${branch}

git checkout -

popd

echo "Create the pull request"
title="datafusion ${version}"
body="Created using \`bump-formula-pr\`"
data="{\"title\":\"$title\", \"body\":\"$body\", \"head\":\"$github_username:$branch\", \"base\":\"$homebrew_default_branch_name\"}"
curl -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $github_token" \
    https://api.github.com/repos/Homebrew/homebrew-core/pulls \
    -d "$data"

echo "Complete!"
