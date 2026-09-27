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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;

use crate::planner::SparkFunctionPlanner;
use crate::{
    all_default_aggregate_functions, all_default_scalar_functions,
    all_default_table_functions, all_default_window_functions,
};

/// Extension trait for adding Apache Spark features to [`SessionStateBuilder`].
///
/// This trait provides a convenient way to register all Apache Spark-compatible
/// functions and planners with a DataFusion session.
///
/// # Example
///
/// ```rust
/// use datafusion::execution::SessionStateBuilder;
/// use datafusion_spark::SessionStateBuilderSpark;
///
/// // Create a SessionState with Apache Spark features enabled
/// // note: the order matters here, `with_spark_features` should be
/// // called after `with_default_features` to overwrite any existing functions
/// let state = SessionStateBuilder::new()
///     .with_default_features()
///     .with_spark_features()
///     .build();
/// ```
pub trait SessionStateBuilderSpark {
    /// Adds all expr_planners, scalar, aggregate, window and table functions
    /// compatible with Apache Spark.
    ///
    /// Note: This overwrites any previously registered items with the same name.
    fn with_spark_features(self) -> Self;
}

impl SessionStateBuilderSpark for SessionStateBuilder {
    fn with_spark_features(mut self) -> Self {
        self.expr_planners()
            .get_or_insert_with(Vec::new)
            // planners are evaluated in order of insertion. Push Apache Spark function planner to the front
            // to take precedence over others
            .insert(0, Arc::new(SparkFunctionPlanner));

        self.scalar_functions()
            .get_or_insert_with(Vec::new)
            .extend(all_default_scalar_functions());

        self.aggregate_functions()
            .get_or_insert_with(Vec::new)
            .extend(all_default_aggregate_functions());

        self.window_functions()
            .get_or_insert_with(Vec::new)
            .extend(all_default_window_functions());

        self.table_functions()
            .get_or_insert_with(HashMap::new)
            .extend(
                all_default_table_functions()
                    .into_iter()
                    .map(|f| (f.name().to_string(), f)),
            );

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::config::Dialect;
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_session_state_with_spark_features() {
        let state = SessionStateBuilder::new().with_spark_features().build();

        assert!(
            state.scalar_functions().contains_key("sha2"),
            "Apache Spark scalar function 'sha2' should be registered"
        );

        assert!(
            state.aggregate_functions().contains_key("try_sum"),
            "Apache Spark aggregate function 'try_sum' should be registered"
        );

        assert!(
            !state.expr_planners().is_empty(),
            "Apache Spark expr planners should be registered"
        );
    }

    #[tokio::test]
    async fn test_spark_dialect_with_spark_functions() {
        let query = "SELECT sha2('abc', 256), CAST(1 AS LONG)";

        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.dialect = Dialect::Spark;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_spark_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        let result = ctx.sql(query).await.unwrap().collect().await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);

        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.dialect = Dialect::Generic;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_spark_features()
            .build();
        let ctx = SessionContext::new_with_state(state);

        let err = ctx.sql(query).await.unwrap_err().to_string();
        assert!(
            err.contains("Unsupported SQL type LONG"),
            "unexpected error: {err}"
        );
    }
}
