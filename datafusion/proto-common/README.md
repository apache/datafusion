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

# `datafusion-proto-common`: Apache DataFusion Protobuf Serialization / Deserialization

This crate contains code to convert Apache [DataFusion] primitive types to and from
bytes, which can be useful for sending data over the network.

See [API Docs] for details and examples.

Most projects should use the [`datafusion-proto`] crate directly, which re-exports
this module. If you are already using the [`datafusion-protp`] crate, there is no
reason to use this crate directly in your project as well.

[`datafusion-proto`]: https://crates.io/crates/datafusion-proto
[datafusion]: https://datafusion.apache.org
[api docs]: http://docs.rs/datafusion-proto/latest
