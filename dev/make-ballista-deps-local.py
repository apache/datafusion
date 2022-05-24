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

import glob
import os
from pathlib import Path

datafusion_root = Path( __file__ ).parent.parent.absolute()

# TODO we could do this dynamically and avoid hard-coding the crates and paths
crate_paths = {
    "datafusion-cli":           "datafusion-cli",
    "datafusion":               "datafusion/core",
    "datafusion-common":        "datafusion/common",
    "datafusion-data-access":   "datafusion/data-access",
    "datafusion-expr":          "datafusion/expr",
    "datafusion-jit":           "datafusion/jit",
    "datafusion-physical-expr": "datafusion/physical-expr",
    "datafusion-proto":         "datafusion/proto",
    "datafusion-row":           "datafusion/row"
}

toml_files = glob.glob("arrow-ballista/**/Cargo.toml", recursive=True)
for file in toml_files:
    print('Updating {}'.format(file))
    toml = open(file, 'r')
    lines = toml.readlines()
    toml = open(file, 'w')
    for line in lines:
        pos = line.find('=')
        if pos != -1:
            crate = line[0:pos].strip()
            if crate in crate_paths:
                path = os.path.join(datafusion_root, crate_paths[crate])
                line = crate + " = { path = \"" + path + "\" }\n"
                print("\tupdating to: {}".format(line))
        toml.write(line)
    toml.close()