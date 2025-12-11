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

/// Add an additional module here for convenience to scope this to only
/// when the feature integration-tests is built
#[cfg(feature = "integration-tests")]
mod tests {
    use arrow::array::Float64Array;
    use datafusion::common::record_batch;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::logical_expr::{AggregateUDF, AggregateUDFImpl};
    use datafusion::prelude::{col, SessionContext};
    use std::sync::Arc;

    use datafusion_ffi::tests::utils::get_module;

    #[tokio::test]
    async fn test_ffi_udaf() -> Result<()> {
        let module = get_module()?;

        let ffi_sum_func =
            module
                .create_sum_udaf()
                .ok_or(DataFusionError::NotImplemented(
                    "External table provider failed to implement create_udaf".to_string(),
                ))?();
        let foreign_sum_func: Arc<dyn AggregateUDFImpl> = (&ffi_sum_func).into();

        let udaf = AggregateUDF::new_from_shared_impl(foreign_sum_func);

        let ctx = SessionContext::default();
        let record_batch = record_batch!(
            ("a", Int32, vec![1, 2, 2, 4, 4, 4, 4]),
            ("b", Float64, vec![1.0, 2.0, 2.0, 4.0, 4.0, 4.0, 4.0])
        )
        .unwrap();

        let df = ctx.read_batch(record_batch)?;

        let df = df
            .aggregate(
                vec![col("a")],
                vec![udaf.call(vec![col("b")]).alias("sum_b")],
            )?
            .sort_by(vec![col("a")])?;

        let result = df.collect().await?;

        let expected = record_batch!(
            ("a", Int32, vec![1, 2, 4]),
            ("sum_b", Float64, vec![1.0, 4.0, 16.0])
        )?;

        assert_eq!(result[0], expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_grouping_udaf() -> Result<()> {
        let module = get_module()?;

        let ffi_stddev_func =
            module
                .create_stddev_udaf()
                .ok_or(DataFusionError::NotImplemented(
                    "External table provider failed to implement create_udaf".to_string(),
                ))?();
        let foreign_stddev_func: Arc<dyn AggregateUDFImpl> = (&ffi_stddev_func).into();

        let udaf = AggregateUDF::new_from_shared_impl(foreign_stddev_func);

        let ctx = SessionContext::default();
        let record_batch = record_batch!(
            ("a", Int32, vec![1, 2, 2, 4, 4, 4, 4]),
            (
                "b",
                Float64,
                vec![
                    1.0,
                    2.0,
                    2.0 + 2.0_f64.sqrt(),
                    4.0,
                    4.0,
                    4.0 + 3.0_f64.sqrt(),
                    4.0 + 3.0_f64.sqrt()
                ]
            )
        )
        .unwrap();

        let df = ctx.read_batch(record_batch)?;

        let df = df
            .aggregate(
                vec![col("a")],
                vec![udaf.call(vec![col("b")]).alias("stddev_b")],
            )?
            .sort_by(vec![col("a")])?;

        let result = df.collect().await?;
        let result = result[0].column_by_name("stddev_b").unwrap();
        let result = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values();

        assert!(result.first().unwrap().is_nan());
        assert!(result.get(1).unwrap() - 1.0 < 0.00001);
        assert!(result.get(2).unwrap() - 1.0 < 0.00001);

        Ok(())
    }
}
