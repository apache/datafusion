#!/bin/bash
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
#

# Adapted from https://github.com/apache/arrow-rs/tree/master/dev/release/create-tarball.sh

# This script creates a signed tarball in
# dev/dist/apache-arrow-datafusion-<version>-<sha>.tar.gz and uploads it to
# the "dev" area of the dist.apache.arrow repository and prepares an
# email for sending to the dev@arrow.apache.org list for a formal
# vote.
#
# See release/README.md for full release instructions
#
# Requirements:
#
# 1. gpg setup for signing and have uploaded your public
# signature to https://pgp.mit.edu/
#
# 2. Logged into the apache svn server with the appropriate
# credentials
#
#
# Based in part on 02-source.sh from apache/arrow
#

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <version> <rc>"
    echo "ex. $0 4.1.0 2"
  exit
fi

version=$1
rc=$2
tag="${version}-rc${rc}"

echo "Attempting to create ${tarball} from tag ${tag}"
release_hash=$(cd "${SOURCE_TOP_DIR}" && git rev-list --max-count=1 ${tag})

release=apache-arrow-datafusion-${version}
distdir=${SOURCE_TOP_DIR}/dev/dist/${release}-rc${rc}
tarname=${release}.tar.gz
tarball=${distdir}/${tarname}
url="https://dist.apache.org/repos/dist/dev/arrow/${release}-rc${rc}"

if [ -z "$release_hash" ]; then
    echo "Cannot continue: unknown git tag: ${tag}"
fi

echo "Draft email for dev@arrow.apache.org mailing list"
echo ""
echo "---------------------------------------------------------"
cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE][RUST][Datafusion] Release Apache Arrow Datafusion ${version} RC${rc}
Hi,

I would like to propose a release of Apache Arrow Datafusion Implementation,
version ${version}.

This release candidate is based on commit: ${release_hash} [1]
The proposed release tarball and signatures are hosted at [2].
The changelog is located at [3].

Please download, verify checksums and signatures, run the unit tests, and vote
on the release. The vote will be open for at least 72 hours.

Only votes from PMC members are binding, but all members of the community are
encouraged to test the release and vote with "(non-binding)".

The standard verification procedure is documented at https://github.com/apache/arrow-datafusion/blob/master/dev/release/README.md#verifying-release-candidates.

[ ] +1 Release this as Apache Arrow Datafusion ${version}
[ ] +0
[ ] -1 Do not release this as Apache Arrow Datafusion ${version} because...

[1]: https://github.com/apache/arrow-datafusion/tree/${release_hash}
[2]: ${url}
[3]: https://github.com/apache/arrow-datafusion/blob/${release_hash}/CHANGELOG.md
MAIL
echo "---------------------------------------------------------"


# create <tarball> containing the files in git at $release_hash
# the files in the tarball are prefixed with {version} (e.g. 4.0.1)
mkdir -p ${distdir}
(cd "${SOURCE_TOP_DIR}" && git archive ${release_hash} --prefix ${release}/ | gzip > ${tarball})

echo "Running rat license checker on ${tarball}"
${SOURCE_DIR}/run-rat.sh ${tarball}

echo "Signing tarball and creating checksums"
gpg --armor --output ${tarball}.asc --detach-sig ${tarball}
# create signing with relative path of tarball
# so that they can be verified with a command such as
#  shasum --check apache-arrow-datafusion-4.1.0-rc2.tar.gz.sha512
(cd ${distdir} && shasum -a 256 ${tarname}) > ${tarball}.sha256
(cd ${distdir} && shasum -a 512 ${tarname}) > ${tarball}.sha512

echo "Uploading to apache dist/dev to ${url}"
svn co --depth=empty https://dist.apache.org/repos/dist/dev/arrow ${SOURCE_TOP_DIR}/dev/dist
svn add ${distdir}
svn ci -m "Apache Arrow Datafusion ${version} ${rc}" ${distdir}

