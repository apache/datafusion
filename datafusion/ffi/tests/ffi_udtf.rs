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
/// when the feature integtation-tests is built
#[cfg(feature = "integration-tests")]
mod tests {

    use std::sync::Arc;

    use arrow::array::{create_array, ArrayRef};
    use arrow::datatypes::DataType;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::logical_expr::ScalarUDF;
    use datafusion::prelude::SessionContext;

    use datafusion_ffi::tests::create_record_batch;
    use datafusion_ffi::tests::utils::get_module;
    use datafusion_ffi::udf::ForeignScalarUDF;
    use datafusion_ffi::udtf::ForeignTableFunction;

    /// This test validates that we can load an external module and use a scalar
    /// udf defined in it via the foreign function interface. In this case we are
    /// using the abs() function as our scalar UDF.
    #[tokio::test]
    async fn test_user_defined_table_function() -> Result<()> {
        let module = get_module()?;

        let ffi_table_func = module
            .create_table_function()
            .ok_or(DataFusionError::NotImplemented(
            "External table function provider failed to implement create_table_function"
                .to_string(),
        ))?();
        let foreign_table_func: ForeignTableFunction = ffi_table_func.into();

        let udtf = Arc::new(foreign_table_func);

        let ctx = SessionContext::default();
        ctx.register_udtf("my_range", udtf);

        let result = ctx
            .sql("SELECT * FROM my_range(5)")
            .await?
            .collect()
            .await?;
        let expected = create_array!(Int64, [0, 1, 2, 3, 4]) as ArrayRef;

        assert!(result.len() == 1);
        assert!(result[0].column(0) == &expected);

        Ok(())
    }

    /// This test validates nullary input UDFs
    #[tokio::test]
    async fn test_nullary_scalar_udf() -> Result<()> {
        let module = get_module()?;

        let ffi_abs_func =
            module
                .create_nullary_udf()
                .ok_or(DataFusionError::NotImplemented(
                    "External table provider failed to implement create_scalar_udf"
                        .to_string(),
                ))?();
        let foreign_abs_func: ForeignScalarUDF = (&ffi_abs_func).try_into()?;

        let udf: ScalarUDF = foreign_abs_func.into();

        let ctx = SessionContext::default();
        let df = ctx.read_batch(create_record_batch(-5, 5))?;

        let df = df.with_column("time_now", udf.call(vec![]))?;

        let result = df.collect().await?;

        assert!(result.len() == 1);
        assert_eq!(
            result[0].column_by_name("time_now").unwrap().data_type(),
            &DataType::Float64
        );

        Ok(())
    }
}
