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

# Profiling Cookbook

The section contains examples how to perform CPU profiling for Apache DataFusion on different operating systems.

## MacOS

### Building a flamegraph

- [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph)

Test:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --root --unit-test datafusion  -- dataframe::tests::test_array_agg
```

Benchmark:

```bash
CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph --root --bench sql_planner -- --bench
```

Open `flamegraph.svg` file with the browser

- dtrace with DataFusion CLI

```bash
git clone https://github.com/brendangregg/FlameGraph.git /tmp/fg
cd datafusion-cli
CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release
echo "select * from table;" >> test.sql
sudo dtrace -c './target/debug/datafusion-cli -f test.sql' -o out.stacks -n 'profile-997 /execname == "datafusion-cli"/ { @[ustack(100)] = count(); }'
/tmp/fg/FlameGraph/stackcollapse.pl out.stacks | /tmp/fg/FlameGraph/flamegraph.pl > flamegraph.svg
```

Open `flamegraph.svg` file with the browser

### CPU profiling with XCode Instruments

[Video: how to CPU profile DataFusion with XCode Instruments](https://youtu.be/P3dXH61Kr5U)

## Linux

## Windows
