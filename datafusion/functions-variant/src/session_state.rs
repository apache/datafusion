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

use datafusion::execution::SessionStateBuilder;

use crate::all_default_variant_functions;

/// Extension trait for adding Parquet Variant scalar functions to
/// [`SessionStateBuilder`].
///
/// # Example
///
/// ```rust
/// use datafusion::execution::SessionStateBuilder;
/// use datafusion_functions_variant::SessionStateBuilderVariant;
///
/// let state = SessionStateBuilder::new()
///     .with_default_features()
///     .with_variant_features()
///     .build();
/// ```
pub trait SessionStateBuilderVariant {
    /// Adds all scalar functions that operate on the Parquet Variant logical
    /// type.
    ///
    /// Note: this overwrites any previously registered items with the same name.
    fn with_variant_features(self) -> Self;
}

impl SessionStateBuilderVariant for SessionStateBuilder {
    fn with_variant_features(mut self) -> Self {
        self.scalar_functions()
            .get_or_insert_with(Vec::new)
            .extend(all_default_variant_functions());

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_state_with_variant_features() {
        let state = SessionStateBuilder::new().with_variant_features().build();

        assert!(
            state.scalar_functions().contains_key("variant_get"),
            "variant scalar function 'variant_get' should be registered"
        );
    }
}
