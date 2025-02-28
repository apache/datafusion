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

echo "This script is experimental! Please read the following completely to understand
what this script does before running it.

This script is designed to regenerate the .slt files in datafusion-testing/data/sqlite/
from source files obtained from a git repository. To do that the following steps are
performed:

- Verify required commands are installed and the PG_URI environment variable is set
- Clone the remote git repository into /tmp/sqlitetesting
- Delete all existing .slt files in datafusion-testing/data/sqlite/ folder
- Copy the .test files from the cloned git repo into datafusion-testing
- Remove a few directories and files from the copied files (not relevant to DataFusion)
- Rename the .test files to .slt and cleanses the files. Cleansing involves:
 - dos2unix
 - removing all references to mysql, mssql and postgresql
 - adds in a new 'control resultmode valuewise' statement at the top of all files
 - updates the files to change 'AS REAL' to 'AS FLOAT8'
- Replace the sqllogictest-rs dependency in the Cargo.toml with a version to
  a git repository that has been custom written to properly complete the files
  with comparison of datafusion results with postgresql
- Replace the sqllogictest.rs file with a customized version that will work with
  the customized sqllogictest-rs dependency
- Run the sqlite test with completion (takes > 1 hr)
- Update a few results to ignore known issues
- Run sqlite test to verify results
- Perform cleanup to revert changes to the Cargo.toml & sqllogictest.rs files
- Delete backup files and the /tmp/sqlitetesting directory
"
read -r -p "Do you understand and accept the risk? (yes/no): " acknowledgement

if [ "${acknowledgement,,}" != "yes" ]; then
  exit 0
else
  echo "Ok, Proceeding..."
fi

if [ ! -x "$(command -v sd)" ]; then
  echo "This script required 'sd' to be installed. Install via 'cargo install sd' or using your local package manager"
  exit 0
else
  echo "sd command is installed, proceeding..."
fi

if [ ! -x "$(command -v rename)" ]; then
  echo "This script required 'rename' to be installed. Install using your local package manager"
  exit 0
else
  echo "rename command is installed, proceeding..."
fi

if [ ! -x "$(command -v dos2unix)" ]; then
  echo "This script required 'dos2unix' to be installed. Install using your local package manager"
  exit 0
else
  echo "dos2unix command is installed, proceeding..."
fi

if [ -z "${PG_URI}" ]; then
  echo "A postgresql database is required for running the sqlite regeneration script.
Please set the PG_URI environment variable to point to an empty postgresql database and retry."
  exit 0
else
  echo "PG_URI was set, proceeding"
fi

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DF_HOME="$SCRIPT_PATH/../../"

# location where we'll clone sql-logic-test repos
if [ ! -d "/tmp/sqlitetesting" ]; then
  mkdir /tmp/sqlitetesting
fi

if [ ! -d "/tmp/sqlitetesting/sql-logic-test" ]; then
  echo "Cloning sql-logic-test into /tmp/sqlitetesting/"
  cd /tmp/sqlitetesting/ || exit;
  git clone https://github.com/hydromatic/sql-logic-test.git
fi

echo "Removing all existing .slt files from datafusion-testing/data/sqlite/ directory"

cd "$DF_HOME/datafusion-testing/data/sqlite/" || exit;
find ./ -type f -name "*.slt" -exec rm {} \;

echo "Copying .test files from sql-logic-test to datafusion-testing/data/sqlite/"

cp -r /tmp/sqlitetesting/sql-logic-test/src/main/resources/test/* ./

echo "Removing 'evidence/*' and 'index/delete/*' directories from datafusion-testing"

find ./evidence/ -type f -name "*.test" -exec rm {} \;
rm -rf ./index/delete/1
rm -rf ./index/delete/10
rm -rf ./index/delete/100
rm -rf ./index/delete/1000
rm -rf ./index/delete/10000
# this file is empty and causes the sqllogictest-rs code to fail
rm ./index/view/10/slt_good_0.test

echo "Renaming .test files to .slt and cleansing the files ..."

# add hash-threshold lines into these 3 files as they were missing
# skip using sed as gnu sed and mac sed are not friends

echo -e "hash-threshold 8\n\n$(cat select1.test)" > select1.test
echo -e "hash-threshold 8\n\n$(cat select4.test)" > select4.test
echo -e "hash-threshold 8\n\n$(cat select5.test)" > select5.test
# rename
find ./ -type f -name "*.test" -exec rename -f 's/\.test/\.slt/' {} \;
# gotta love windows :/
find ./ -type f -name "*.slt" -exec dos2unix --quiet {} \;
# add in control resultmode
find ./ -type f -name "*.slt" -exec sd -f i 'hash-threshold 8\n' 'hash-threshold 8\ncontrol resultmode valuewise\n' {} \;
# remove mysql tests and skipif lines
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mysql.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'skipif mysql.+\n' '' {} \;
# remove postgres skipif
find ./ -type f -name "*.slt" -exec sd -f i 'skipif postgresql(.+)\n' '' {} \;
# remove mssql tests
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'onlyif mssql.+\n.+\n.+\n\n' '' {} \;
find ./ -type f -name "*.slt" -exec sd -f i 'skipif mssql # not compatible\n' '' {} \;
# change REAL datatype to FLOAT8
find ./ -type f -name "*.slt" -exec sd -f i 'AS REAL' 'AS FLOAT8' {} \;

echo "Updating the datafusion/sqllogictest/Cargo.toml file with an updated sqllogictest dependency"

# update the sqllogictest Cargo.toml with the new repo for sqllogictest-rs (tied to a specific hash)
cd "$DF_HOME" || exit;
sd -f i '^sqllogictest.*' 'sqllogictest = { git = "https://github.com/Omega359/sqllogictest-rs.git", rev = "73c47cf7" }' datafusion/sqllogictest/Cargo.toml

echo "Replacing the datafusion/sqllogictest/bin/sqllogictests.rs file with a custom version required for running completion"

# replace the sqllogictest.rs with a customized version.
cp datafusion/sqllogictest/regenerate/sqllogictests.rs datafusion/sqllogictest/bin/sqllogictests.rs

echo "Running the sqllogictests with sqlite completion. This will take approximately an hour to run"

cargo test --profile release-nonlto --features postgres --test sqllogictests -- --include-sqlite --postgres-runner --complete

if [ $? -eq 0 ]; then
  echo "Applying patches for #13784 (https://github.com/apache/datafusion/issues/13784)"

  sd -f i 'query I rowsort label-2475\n' '# Datafusion - #13784 - https://github.com/apache/datafusion/issues/13784\nskipif Datafusion\nquery I rowsort label-2475\n' datafusion-testing/data/sqlite/random/aggregates/slt_good_102.slt
  sd -f i 'query I rowsort label-3738\n' '# Datafusion - #13784 - https://github.com/apache/datafusion/issues/13784\nskipif Datafusion\nquery I rowsort label-3738\n' datafusion-testing/data/sqlite/random/aggregates/slt_good_112.slt

  echo "Running the sqllogictests with sqlite files. This will take approximately 20 minutes to run"

  cargo test --profile release-nonlto --test sqllogictests -- --include-sqlite

  if [ $? -eq 0 ]; then
	echo "Sqlite tests completed successfully. The datafusion-testing git submodule is ready to be pushed to a new remote and a PR created in https://github.com/apache/datafusion-testing/"

  else
	echo "Completion of sqlite test files failed. Please correct the issues in the .slt files and run the test again using the command 'cargo test --profile release-nonlto --test sqllogictests -- --include-sqlite'"
  fi
else
  echo "Completion of sqlite test files failed!"
fi

echo "Cleaning up source code changes and temporary files and directories"

cd "$DF_HOME" || exit;
find ./datafusion-testing/data/sqlite/ -type f -name "*.bak" -exec rm {} \;
find ./datafusion/sqllogictest/test_files/pg_compat/ -type f -name "*.bak" -exec rm {} \;
git checkout datafusion/sqllogictest/Cargo.toml
git checkout datafusion/sqllogictest/bin/sqllogictests.rs
rm -rf /tmp/sqlitetesting
