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

//! This example shows how to create and use "Async UDFs" in DataFusion.
//!
//! Async UDFs allow you to perform asynchronous operations, such as
//! making network requests. This can be used for tasks like fetching
//! data from an external API such as a LLM service or an external database.

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::assert_batches_eq;
use datafusion::common::cast::as_string_view_array;
use datafusion::common::error::Result;
use datafusion::common::not_impl_err;
use datafusion::common::utils::take_function_args;
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::any::Any;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Use a hard coded parallelism level of 4 so the explain plan
    // is consistent across machines.
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx =
        SessionContext::from(SessionStateBuilder::new().with_config(config).build());

    // Similarly to regular UDFs, you create an AsyncScalarUDF by implementing
    // `AsyncScalarUDFImpl` and creating an instance of `AsyncScalarUDF`.
    let async_equal = AskLLM::new();
    let udf = AsyncScalarUDF::new(Arc::new(async_equal));

    // Async UDFs are registered with the SessionContext, using the same
    // `register_udf` method as regular UDFs.
    ctx.register_udf(udf.into_scalar_udf());

    // Create a table named 'animal' with some sample data
    ctx.register_batch("animal", animal()?)?;

    // You can use the async UDF as normal in SQL queries
    //
    // Note: Async UDFs can currently be used in the select list and filter conditions.
    let results = ctx
        .sql("select * from animal a where ask_llm(a.name, 'Is this animal furry?')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        [
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 1  | cat  |",
            "| 2  | dog  |",
            "+----+------+",
        ],
        &results
    );

    // While the interface is the same for both normal and async UDFs, you can
    // use `EXPLAIN` output to see that the async UDF uses a special
    // `AsyncFuncExec` node in the physical plan:
    let results = ctx
        .sql("explain select * from animal a where ask_llm(a.name, 'Is this animal furry?')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        [
    "+---------------+--------------------------------------------------------------------------------------------------------------------------------+",
    "| plan_type     | plan                                                                                                                           |",
    "+---------------+--------------------------------------------------------------------------------------------------------------------------------+",
    "| logical_plan  | SubqueryAlias: a                                                                                                               |",
    "|               |   Filter: ask_llm(CAST(animal.name AS Utf8View), Utf8View(\"Is this animal furry?\"))                                            |",
    "|               |     TableScan: animal projection=[id, name]                                                                                    |",
    "| physical_plan | CoalesceBatchesExec: target_batch_size=8192                                                                                    |",
    "|               |   FilterExec: __async_fn_0@2, projection=[id@0, name@1]                                                                        |",
    "|               |     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1                                                       |",
    "|               |       AsyncFuncExec: async_expr=[async_expr(name=__async_fn_0, expr=ask_llm(CAST(name@1 AS Utf8View), Is this animal furry?))] |",
    "|               |         CoalesceBatchesExec: target_batch_size=8192                                                                            |",
    "|               |           DataSourceExec: partitions=1, partition_sizes=[1]                                                                    |",
    "|               |                                                                                                                                |",
    "+---------------+--------------------------------------------------------------------------------------------------------------------------------+",
        ],
        &results
    );

    Ok(())
}

/// Returns a sample `RecordBatch` representing an "animal" table with two columns:
fn animal() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let name_array = Arc::new(StringArray::from(vec![
        "cat", "dog", "fish", "bird", "snake",
    ]));

    Ok(RecordBatch::try_new(schema, vec![id_array, name_array])?)
}

/// An async UDF that simulates asking a large language model (LLM) service a
/// question based on the content of two columns. The UDF will return a boolean
/// indicating whether the LLM thinks the first argument matches the question in
/// the second argument.
///
/// Since this is a simplified example, it does not call an LLM service, but
/// could be extended to do so in a real-world scenario.
#[derive(Debug)]
struct AskLLM {
    signature: Signature,
}

impl Default for AskLLM {
    fn default() -> Self {
        Self::new()
    }
}

impl AskLLM {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8View, DataType::Utf8View],
                Volatility::Volatile,
            ),
        }
    }
}

/// All async UDFs implement the `ScalarUDFImpl` trait, which provides the basic
/// information for the function, such as its name, signature, and return type.
/// [async_trait]
impl ScalarUDFImpl for AskLLM {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ask_llm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    /// Since this is an async UDF, the `invoke_with_args` method will not be
    /// called directly.
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        not_impl_err!("AskLLM can only be called from async contexts")
    }
}

/// In addition to [`ScalarUDFImpl`], we also need to implement the
/// [`AsyncScalarUDFImpl`] trait.
#[async_trait]
impl AsyncScalarUDFImpl for AskLLM {
    /// The `invoke_async_with_args` method is similar to `invoke_with_args`,
    /// but it returns a `Future` that resolves to the result.
    ///
    /// Since this signature is `async`, it can do any `async` operations, such
    /// as network requests. This method is run on the same tokio `Runtime` that
    /// is processing the query, so you may wish to make actual network requests
    /// on a different `Runtime`, as explained in the `thread_pools.rs` example
    /// in this directory.
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ArrayRef> {
        // in a real UDF you would likely want to special case constant
        // arguments to improve performance, but this example converts the
        // arguments to arrays for simplicity.
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [content_column, question_column] = take_function_args(self.name(), args)?;

        // In a real function, you would use a library such as `reqwest` here to
        // make an async HTTP request. Credentials and other configurations can
        // be supplied via the `ConfigOptions` parameter.

        // In this example, we will simulate the LLM response by comparing the two
        // input arguments using some static strings
        let content_column = as_string_view_array(&content_column)?;
        let question_column = as_string_view_array(&question_column)?;

        let result_array: BooleanArray = content_column
            .iter()
            .zip(question_column.iter())
            .map(|(a, b)| {
                // If either value is null, return None
                let a = a?;
                let b = b?;
                // Simulate an LLM response by checking the arguments to some
                // hardcoded conditions.
                if a.contains("cat") && b.contains("furry")
                    || a.contains("dog") && b.contains("furry")
                {
                    Some(true)
                } else {
                    Some(false)
                }
            })
            .collect();

        Ok(Arc::new(result_array))
    }
}
