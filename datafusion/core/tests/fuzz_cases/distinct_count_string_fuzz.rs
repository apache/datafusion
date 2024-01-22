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

//! Compare DistinctCount for string with naive HashSet and Short String Optimized HashSet

use std::any::Any;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow_array::cast::{as_list_array, as_string_array};
use arrow_array::{ListArray, StringArray};
use datafusion::physical_expr::aggregate::utils::down_cast_any_ref;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_common::utils::array_into_list_array;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use datafusion_physical_expr::expressions::format_state_name;
use datafusion_physical_expr::PhysicalExpr;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;

use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_physical_expr::expressions::{col, DistinctCount};
use datafusion_physical_expr::AggregateExpr;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn distinct_count_string_test() {
    // max length of generated strings
    let max_lens = [4, 8, 16, 32, 64, 128, 256, 512];
    let n = 300;
    // number of rows in each batch
    let row_lens = [8, 16, 32, 64, 128, 256, 512, 1024];
    for row_len in row_lens {
        let mut handles = Vec::new();
        for i in 0..n {
            let test_idx = i % max_lens.len();
            let max_len = max_lens[test_idx];
            let job = tokio::spawn(run_distinct_count_test(make_staggered_batches(
                max_len, row_len, i as u64,
            )));
            handles.push(job);
        }
        for job in handles {
            job.await.unwrap();
        }
    }
}

/// Perform batch and streaming aggregation with same input
/// and verify outputs of `AggregateExec` with pipeline breaking stream `GroupedHashAggregateStream`
/// and non-pipeline breaking stream `BoundedAggregateStream` produces same result.
async fn run_distinct_count_test(input1: Vec<RecordBatch>) {
    let schema = input1[0].schema();
    let session_config = SessionConfig::new().with_batch_size(50);
    let ctx = SessionContext::new_with_config(session_config);

    let control_group_source =
        Arc::new(MemoryExec::try_new(&[input1.clone()], schema.clone(), None).unwrap());

    let experimental_source =
        Arc::new(MemoryExec::try_new(&[input1.clone()], schema.clone(), None).unwrap());

    let distinct_count_expr = vec![
        Arc::new(DistinctCount::new(
            DataType::Utf8,
            col("a", &schema).unwrap(),
            "distinct_count1",
        )) as Arc<dyn AggregateExpr>,
        Arc::new(DistinctCount::new(
            DataType::Utf8,
            col("b", &schema).unwrap(),
            "distinct_count2",
        )) as Arc<dyn AggregateExpr>,
    ];

    let expr = vec![(col("c", &schema).unwrap(), "c".to_string())];
    let group_by = PhysicalGroupBy::new_single(expr);

    let mode = AggregateMode::FinalPartitioned;
    let filter_expr = vec![None; distinct_count_expr.len()];

    let distinct_count_control_group = Arc::new(
        AggregateExec::try_new(
            mode,
            group_by.clone(),
            distinct_count_expr,
            filter_expr.clone(),
            experimental_source,
            schema.clone(),
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;

    let distinct_count_expr = vec![
        Arc::new(DistinctCountForTest::new(
            DataType::Utf8,
            col("a", &schema).unwrap(),
            "distinct_count1",
        )) as Arc<dyn AggregateExpr>,
        Arc::new(DistinctCountForTest::new(
            DataType::Utf8,
            col("b", &schema).unwrap(),
            "distinct_count2",
        )) as Arc<dyn AggregateExpr>,
    ];

    let distinct_count_experimental_group = Arc::new(
        AggregateExec::try_new(
            mode,
            group_by,
            distinct_count_expr,
            filter_expr,
            control_group_source,
            schema,
        )
        .unwrap(),
    ) as Arc<dyn ExecutionPlan>;

    let task_ctx = ctx.task_ctx();
    let collected_control_group =
        collect(distinct_count_experimental_group.clone(), task_ctx.clone())
            .await
            .unwrap();

    let collected_experimental_group =
        collect(distinct_count_control_group.clone(), task_ctx.clone())
            .await
            .unwrap();

    assert_eq!(collected_control_group, collected_experimental_group);
}

fn make_staggered_batches(
    max_len: usize,
    row_len: usize,
    random_seed: u64,
) -> Vec<RecordBatch> {
    // use a random number generator to pick a random sized output
    let mut rng = StdRng::seed_from_u64(random_seed);

    fn gen_data(rng: &mut StdRng, row_len: usize, max_len: usize) -> ListArray {
        let data: Vec<String> = (0..row_len)
            .map(|_| {
                let len = rng.gen_range(0..max_len) + 1;
                "a".repeat(len)
            })
            .collect();
        array_into_list_array(Arc::new(StringArray::from(data)))
    }

    let inputa = gen_data(&mut rng, row_len, max_len);
    let inputb = gen_data(&mut rng, row_len, max_len);
    let input_groupby = gen_data(&mut rng, row_len, max_len);
    let batch = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(inputa) as ArrayRef),
        ("b", Arc::new(inputb) as ArrayRef),
        ("c", Arc::new(input_groupby) as ArrayRef), // column for group by
    ])
    .unwrap();

    vec![batch]
}

/// Expression for a COUNT(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctCountForTest {
    /// Column name
    name: String,
    /// The DataType used to hold the state for each input
    state_data_type: DataType,
    /// The input arguments
    expr: Arc<dyn PhysicalExpr>,
}

impl DistinctCountForTest {
    /// Create a new COUNT(DISTINCT) aggregate function.
    pub fn new(
        input_data_type: DataType,
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            state_data_type: input_data_type,
            expr,
        }
    }
}
impl AggregateExpr for DistinctCountForTest {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Int64, true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "count distinct"),
            Field::new("item", self.state_data_type.clone(), true),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        match &self.state_data_type {
            DataType::Utf8 => Ok(Box::new(StringDistinctCountAccumulatorForTest::new())),
            _ => panic!(
                "Unsupported type for COUNT(DISTINCT): {:?}",
                self.state_data_type
            ),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for DistinctCountForTest {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.state_data_type == x.state_data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct StringDistinctCountAccumulatorForTest(HashSet<String>);
impl StringDistinctCountAccumulatorForTest {
    fn new() -> Self {
        Self(HashSet::new())
    }
}

impl Accumulator for StringDistinctCountAccumulatorForTest {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let arr =
            StringArray::from(self.0.iter().map(|s| s.as_str()).collect::<Vec<&str>>());
        let list = Arc::new(array_into_list_array(Arc::new(arr)));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = as_string_array(&values[0]);
        for v in array.iter().flatten() {
            self.0.insert(v.to_string());
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0]);
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let array = as_string_array(&list);
                for v in array.iter().flatten() {
                    self.0.insert(v.to_string());
                }
            };
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0.len() as i64)))
    }

    fn size(&self) -> usize {
        // Size of accumulator
        // + SSOStringHashSet size
        std::mem::size_of_val(self) + self.0.capacity() * std::mem::size_of::<String>()
    }
}
