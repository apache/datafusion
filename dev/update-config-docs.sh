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

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SOURCE_DIR}/../" && pwd

TARGET_FILE="docs/source/user-guide/configs.md"
PRINT_DOCS_COMMAND="cargo run --manifest-path datafusion/core/Cargo.toml --bin print_config_docs"

echo "Inserting header"
cat <<'EOF' > "$TARGET_FILE"
<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

<!---
This file was generated by the dev/update-config-docs.sh script.
Do not edit it manually as changes will be overwritten.
Instead, edit dev/update-config-docs.sh or the docstrings in datafusion/core/src/config.rs.
-->

# Configuration Settings

The following configuration options can be passed to `SessionConfig` to control various aspects of query execution.

EOF

echo "Running CLI and inserting docs table"
$PRINT_DOCS_COMMAND >> "$TARGET_FILE"

echo "Running prettier"
npx prettier@2.3.2 --write "$TARGET_FILE"

echo "'$TARGET_FILE' successfully updated!"
