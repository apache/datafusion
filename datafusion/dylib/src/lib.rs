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

//! Forces `datafusion` to be built and linked as a dynamic library. Modeled on
//! `bevy_dylib`. Enable via the `dynamic` feature on the `datafusion` crate:
//!
//! ```toml
//! datafusion = { version = "...", features = ["dynamic"] }
//! ```
//!
//! Do not enable this in release/distribution builds — the resulting binary
//! will depend on `libdatafusion_dylib.so` at runtime.

// The core crate's package name is `datafusion-core` but its lib name is
// `datafusion` (see datafusion/core/Cargo.toml [lib] section).
#[allow(unused_imports)]
use datafusion as _;
