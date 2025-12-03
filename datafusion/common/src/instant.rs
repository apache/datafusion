// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! WASM-compatible `Instant` wrapper.

#[cfg(target_family = "wasm")]
/// DataFusion wrapper around [`std::time::Instant`]. Uses [`web_time::Instant`]
/// under `wasm` feature gate. It provides the same API as [`std::time::Instant`].
pub type Instant = web_time::Instant;

#[expect(clippy::disallowed_types)]
#[cfg(not(target_family = "wasm"))]
/// DataFusion wrapper around [`std::time::Instant`]. This is only a type alias.
pub type Instant = std::time::Instant;
