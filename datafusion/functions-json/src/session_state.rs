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

use crate::all_default_json_functions;

/// Extension trait for adding JSON function support to [`SessionStateBuilder`].
///
/// This trait provides a convenient way to register all JSON scalar functions
/// with a DataFusion session.
///
/// # Example
///
/// ```rust
/// use datafusion::execution::SessionStateBuilder;
/// use datafusion_functions_json::SessionStateBuilderJson;
///
/// // Create a SessionState with JSON functions enabled
/// // note: the order matters here, `with_json_features` should be
/// // called after `with_default_features` to overwrite any existing functions
/// let state = SessionStateBuilder::new()
///     .with_default_features()
///     .with_json_features()
///     .build();
/// ```
pub trait SessionStateBuilderJson {
    /// Adds all scalar JSON functions.
    ///
    /// Note: This overwrites any previously registered items with the same name.
    fn with_json_features(self) -> Self;
}

impl SessionStateBuilderJson for SessionStateBuilder {
    fn with_json_features(mut self) -> Self {
        self.scalar_functions()
            .get_or_insert_with(Vec::new)
            .extend(all_default_json_functions());

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_session_state_with_json_features() {
        let state = SessionStateBuilder::new().with_json_features().build();

        assert!(
            state.scalar_functions().contains_key("json_get_str"),
            "JSON scalar function 'json_get_str' should be registered"
        );
    }

    #[tokio::test]
    async fn test_json_functions_via_sql() {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_json_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        let result = ctx
            .sql(r#"SELECT json_get_str('{"a": "hello"}', 'a') AS v"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }
}
