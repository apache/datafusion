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

// `datafusion.rs` references `super::datafusion_common::*` for the types it
// imports across packages; alias the generated common module to that name.
// `self::` disambiguates the inner module from the `datafusion-common` crate
// dep, which prost-build's `datafusion_common` crate name otherwise shadows.
pub use self::common_pkg as datafusion_common;

// This code is generated so we don't want to fix any lint violations manually
#[allow(clippy::allow_attributes)]
#[allow(clippy::all)]
#[rustfmt::skip]
pub mod common_pkg {
    include!("datafusion_common.rs");

    #[cfg(feature = "json")]
    include!("datafusion_common.serde.rs");
}

#[allow(clippy::allow_attributes)]
#[allow(clippy::all)]
#[rustfmt::skip]
pub mod datafusion {
    include!("datafusion.rs");

    // Re-export the `datafusion_common` package types from inside `mod datafusion`
    // so historical paths like `datafusion_proto::generated::datafusion::JoinType`
    // (which used to be embedded duplicates of the common types) keep resolving.
    pub use super::common_pkg::*;

    #[cfg(feature = "json")]
    include!("datafusion.serde.rs");
}
