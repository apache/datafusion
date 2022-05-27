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

# DataFusion C API examples

## How to run

Build `libdatafusion_c.so`:

```bash
cargo build
```

C example:

```
cc \
  -o target/debug/sql \
  -I datafusion/c/include \
  datafusion/c/examples/sql.c \
  -L target/debug \
  -Wl,--rpath=target/debug \
  -ldatafusion_c
target/debug/sql
```

Output:

```text
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+
```

Python example:

```bash
LD_LIBRARY_PATH=$PWD/target/debug datafusion/c/examples/sql.py
```

Output:

```text
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+
```

Ruby example:

```bash
LD_LIBRARY_PATH=$PWD/target/debug datafusion/c/examples/sql.rb
```

Output:

```text
+----------+
| Int64(1) |
+----------+
| 1        |
+----------+
```
