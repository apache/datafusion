// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod tests {

    use std::sync::Arc;

    use arrow::array::{ArrayRef, create_array};
    use datafusion::catalog::TableFunctionImpl;
    use datafusion::error::Result;
    use datafusion_ffi::tests::utils::get_module;

    /// This test validates that we can load an external module and use a scalar
    /// udf defined in it via the foreign function interface. In this case we are
    /// using the abs() function as our scalar UDF.
    #[tokio::test]
    async fn test_user_defined_table_function() -> Result<()> {
        let module = get_module()?;
        let (ctx, codec) = crate::utils::ctx_and_codec();

        let ffi_table_func = (module.create_table_function)(codec);
        let foreign_table_func: Arc<dyn TableFunctionImpl> = ffi_table_func.into();

        ctx.register_udtf("my_range", foreign_table_func);

        let result = ctx
            .sql("SELECT * FROM my_range(5)")
            .await?
            .collect()
            .await?;
        let expected = create_array!(Int64, [0, 1, 2, 3, 4]) as ArrayRef;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column(0), &expected);

        Ok(())
    }
}
