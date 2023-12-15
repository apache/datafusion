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

//! Built in optional function packages for DataFusion
//!
//! Each module should implement a "function package" that should have a function with the signature:
//!
//! ```
//! # use std::sync::Arc;
//! # use datafusion_expr::FunctionImplementation;
//! // return a list of functions or stubs
//! fn functions() -> Vec<Arc<dyn FunctionImplementation + Send + Sync>> {
//!    todo!()
//! }
//! ```
//!
//! Which returns:
//!
//! 1. The list of actual function implementation when the relevant
//! feature is activated,
//!
//! 2. A list of stub function when the feature is not activated that produce
//! a runtime error (and explain what feature flag is needed to activate them).
//!
//! The rationale for providing stub functions is to help users to configure datafusion
//! properly (so they get an error telling them why a function is not available)
//! instead of getting a cryptic "no function found" message at runtime.
use datafusion_common::Result;
use datafusion_execution::FunctionRegistry;
use log::debug;

// Macro creates the named module if the feature is enabled
// otherwise creates a stub
macro_rules! make_package {
    ($name:ident, $feature:literal) => {
        #[cfg(feature = $feature)]
        pub mod $name;

        #[cfg(not(feature = $feature))]
        /// Stub module when feature is not enabled
        mod $name {
            use datafusion_expr::ScalarUDF;
            use log::debug;
            use std::sync::Arc;

            pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
                debug!("{} functions disabled", stringify!($name));
                vec![]
            }
        }
    };
}

make_package!(encoding, "encoding_expressions");

pub mod stub;

/// reexports of all expr_fn APIs
pub mod expr_fn {
    #[cfg(feature = "encoding_expressions")]
    pub use super::encoding::expr_fn::*;
}

pub fn register_all(registry: &mut dyn FunctionRegistry) -> Result<()> {
    encoding::functions().into_iter().try_for_each(|udf| {
        let existing_udf = registry.register_udf(udf)?;
        if let Some(existing_udf) = existing_udf {
            debug!("Overwrite existing UDF: {}", existing_udf.name());
        }
        Ok(()) as Result<()>
    })?;
    Ok(())
}
