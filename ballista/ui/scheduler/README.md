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

# Ballista UI

## Start project from source

### Run scheduler/executor

First, run scheduler from project:

```shell
$ cd rust/scheduler
$ RUST_LOG=info cargo run --release
...
    Finished release [optimized] target(s) in 11.92s
    Running `/path-to-project/target/release/ballista-scheduler`
```

and run executor in new terminal:

```shell
$ cd rust/executor
$ RUST_LOG=info cargo run --release
    Finished release [optimized] target(s) in 0.09s
    Running `/path-to-project/target/release/ballista-executor`
```

### Run Client project

```shell
$ cd ui/scheduler
$ yarn
Resolving packages...
$ yarn start
Starting the development server...
```

Now access to http://localhost:3000/
