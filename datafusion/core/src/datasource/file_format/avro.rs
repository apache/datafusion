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

//! Re-exports the [`datafusion_datasource_avro::file_format`] module, and contains tests for it.

pub use datafusion_datasource_avro::file_format::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        datasource::file_format::test_util::scan_format, prelude::SessionContext,
    };
    use arrow::array::{as_string_array, Array};
    use datafusion_catalog::Session;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{
        cast::{
            as_binary_array, as_boolean_array, as_float32_array, as_float64_array,
            as_int32_array, as_timestamp_microsecond_array,
        },
        test_util, Result,
    };

    use datafusion_datasource_avro::AvroFormat;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_plan::{collect, ExecutionPlan};
    use futures::StreamExt;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 4 /* 8/2 */);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.avro", projection, Some(1)).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "id: Int32",
                "bool_col: Boolean",
                "tinyint_col: Int32",
                "smallint_col: Int32",
                "int_col: Int32",
                "bigint_col: Int64",
                "float_col: Float32",
                "double_col: Float64",
                "date_string_col: Binary",
                "string_col: Binary",
                "timestamp_col: Timestamp(Microsecond, None)",
            ],
            x
        );

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);

        assert_snapshot!(batches_to_string(&batches),@r###"
            +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
            | id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |
            +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
            | 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00 |
            | 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01T00:01:00 |
            | 6  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30342f30312f3039 | 30         | 2009-04-01T00:00:00 |
            | 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01T00:01:00 |
            | 2  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30322f30312f3039 | 30         | 2009-02-01T00:00:00 |
            | 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01T00:01:00 |
            | 0  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30312f30312f3039 | 30         | 2009-01-01T00:00:00 |
            | 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01T00:01:00 |
            +----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+
        "###);
        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![1]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_boolean_array(batches[0].column(0))?;
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_null_bool_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![2]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_boolean_array(batches[0].column(0))?;

        assert!(array.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_int32_array(batches[0].column(0))?;
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_null_i32_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![1]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_int32_array(batches[0].column(0))?;

        assert!(array.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![10]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_timestamp_microsecond_array(batches[0].column(0))?;
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000, 1235865660000000, 1238544000000000, 1238544060000000, 1233446400000000, 1233446460000000, 1230768000000000, 1230768060000000]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![6]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float32_array(batches[0].column(0))?;
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![7]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float64_array(batches[0].column(0))?;
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![9]);
        let exec = get_exec(&state, "alltypes_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_binary_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_null_binary_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![6]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_binary_array(batches[0].column(0))?;

        assert!(array.is_null(0));

        Ok(())
    }

    #[tokio::test]
    async fn read_null_string_alltypes_plain_avro() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec =
            get_exec(&state, "alltypes_nulls_plain.avro", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        let array = as_string_array(batches[0].column(0));

        assert!(array.is_null(0));

        Ok(())
    }

    async fn get_exec(
        state: &dyn Session,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = test_util::arrow_test_data();
        let store_root = format!("{testdata}/avro");
        let format = AvroFormat {};
        scan_format(state, &format, &store_root, file_name, projection, limit).await
    }
}
