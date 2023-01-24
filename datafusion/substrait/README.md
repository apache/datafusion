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

# DataFusion + Substrait

[Substrait](https://substrait.io/) provides a cross-language serialization format for relational algebra, based on
protocol buffers.

This repository provides a Substrait producer and consumer for DataFusion:

- The producer converts a DataFusion logical plan into a Substrait protobuf.
- The consumer converts a Substrait protobuf into a DataFusion logical plan.

Potential uses of this crate:

- Replace the current [DataFusion protobuf definition](https://github.com/apache/arrow-datafusion/blob/master/datafusion-proto/proto/datafusion.proto) used in Ballista for passing query plan fragments to executors
- Make it easier to pass query plans over FFI boundaries, such as from Python to Rust
- Allow Apache Calcite query plans to be executed in DataFusion
