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

//! This module contains end to end tests of creating
//! user defined window functions

use std::{
    ops::Range,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use arrow::array::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use datafusion::{assert_batches_eq, prelude::SessionContext};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    function::PartitionEvaluatorFactory, window_state::WindowAggState,
    PartitionEvaluator, ReturnTypeFunction, Signature, Volatility, WindowUDF,
};

/// A query with a window function evaluated over the entire partition
const UNBOUNDED_WINDOW_QUERY: &str = "SELECT x, y, val, \
     odd_counter(val) OVER (PARTITION BY x ORDER BY y) \
     from t ORDER BY x, y";

/// A query with a window function evaluated over a moving window
const BOUNDED_WINDOW_QUERY:  &str  =
    "SELECT x, y, val, \
     odd_counter(val) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
     from t ORDER BY x, y";

/// Test to show the contents of the setup
#[tokio::test]
async fn test_setup() {
    let test_state = TestState::new();
    let TestContext { ctx, test_state: _ } = TestContext::new(test_state);

    let sql = "SELECT * from t order by x, y";
    let expected = vec![
        "+---+---+-----+",
        "| x | y | val |",
        "+---+---+-----+",
        "| 1 | a | 0   |",
        "| 1 | b | 1   |",
        "| 1 | c | 2   |",
        "| 2 | d | 3   |",
        "| 2 | e | 4   |",
        "| 2 | f | 5   |",
        "| 2 | g | 6   |",
        "| 2 | h | 6   |",
        "| 2 | i | 6   |",
        "| 2 | j | 6   |",
        "+---+---+-----+",
    ];
    assert_batches_eq!(expected, &execute(&ctx, sql).await.unwrap());
}

/// Basic user defined window function
#[tokio::test]
async fn test_udwf() {
    let test_state = TestState::new();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let expected = vec![
        "+---+---+-----+--------------------+",
        "| x | y | val | odd_counter(t.val) |",
        "+---+---+-----+--------------------+",
        "| 1 | a | 0   | 1                  |",
        "| 1 | b | 1   | 1                  |",
        "| 1 | c | 2   | 1                  |",
        "| 2 | d | 3   | 2                  |",
        "| 2 | e | 4   | 2                  |",
        "| 2 | f | 5   | 2                  |",
        "| 2 | g | 6   | 2                  |",
        "| 2 | h | 6   | 2                  |",
        "| 2 | i | 6   | 2                  |",
        "| 2 | j | 6   | 2                  |",
        "+---+---+-----+--------------------+",
    ];
    assert_batches_eq!(
        expected,
        &execute(&ctx, UNBOUNDED_WINDOW_QUERY).await.unwrap()
    );
    // evaluated on two distinct batches
    assert_eq!(test_state.evaluate_all_called(), 2);
}

/// Basic user defined window function with bounded window
#[tokio::test]
async fn test_udwf_bounded_window_ignores_frame() {
    let test_state = TestState::new();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    // Since the UDWF doesn't say it needs the window frame, the frame is ignored
    let expected = vec![
        "+---+---+-----+--------------------+",
        "| x | y | val | odd_counter(t.val) |",
        "+---+---+-----+--------------------+",
        "| 1 | a | 0   | 1                  |",
        "| 1 | b | 1   | 1                  |",
        "| 1 | c | 2   | 1                  |",
        "| 2 | d | 3   | 2                  |",
        "| 2 | e | 4   | 2                  |",
        "| 2 | f | 5   | 2                  |",
        "| 2 | g | 6   | 2                  |",
        "| 2 | h | 6   | 2                  |",
        "| 2 | i | 6   | 2                  |",
        "| 2 | j | 6   | 2                  |",
        "+---+---+-----+--------------------+",
    ];
    assert_batches_eq!(
        expected,
        &execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap()
    );
    // evaluated on 2 distinct batches (when x=1 and x=2)
    assert_eq!(test_state.evaluate_called(), 0);
    assert_eq!(test_state.evaluate_all_called(), 2);
}

/// Basic user defined window function with bounded window
#[tokio::test]
async fn test_udwf_bounded_window() {
    let test_state = TestState::new().with_uses_window_frame();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let expected = vec![
        "+---+---+-----+--------------------+",
        "| x | y | val | odd_counter(t.val) |",
        "+---+---+-----+--------------------+",
        "| 1 | a | 0   | 1                  |",
        "| 1 | b | 1   | 1                  |",
        "| 1 | c | 2   | 1                  |",
        "| 2 | d | 3   | 1                  |",
        "| 2 | e | 4   | 2                  |",
        "| 2 | f | 5   | 1                  |",
        "| 2 | g | 6   | 1                  |",
        "| 2 | h | 6   | 0                  |",
        "| 2 | i | 6   | 0                  |",
        "| 2 | j | 6   | 0                  |",
        "+---+---+-----+--------------------+",
    ];
    assert_batches_eq!(
        expected,
        &execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap()
    );
    // Evaluate is called for each input rows
    assert_eq!(test_state.evaluate_called(), 10);
    assert_eq!(test_state.evaluate_all_called(), 0);
}

/// Basic stateful user defined window function
#[tokio::test]
async fn test_stateful_udwf() {
    let test_state = TestState::new()
        .with_supports_bounded_execution()
        .with_uses_window_frame();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let expected = vec![
        "+---+---+-----+--------------------+",
        "| x | y | val | odd_counter(t.val) |",
        "+---+---+-----+--------------------+",
        "| 1 | a | 0   | 0                  |",
        "| 1 | b | 1   | 1                  |",
        "| 1 | c | 2   | 1                  |",
        "| 2 | d | 3   | 1                  |",
        "| 2 | e | 4   | 1                  |",
        "| 2 | f | 5   | 2                  |",
        "| 2 | g | 6   | 2                  |",
        "| 2 | h | 6   | 2                  |",
        "| 2 | i | 6   | 2                  |",
        "| 2 | j | 6   | 2                  |",
        "+---+---+-----+--------------------+",
    ];
    assert_batches_eq!(
        expected,
        &execute(&ctx, UNBOUNDED_WINDOW_QUERY).await.unwrap()
    );
    assert_eq!(test_state.evaluate_called(), 10);
    assert_eq!(test_state.update_state_called(), 10);
    assert_eq!(test_state.evaluate_all_called(), 0);
}

/// Basic stateful user defined window function with bounded window
#[tokio::test]
async fn test_stateful_udwf_bounded_window() {
    let test_state = TestState::new()
        .with_supports_bounded_execution()
        .with_uses_window_frame();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let expected = vec![
        "+---+---+-----+--------------------+",
        "| x | y | val | odd_counter(t.val) |",
        "+---+---+-----+--------------------+",
        "| 1 | a | 0   | 1                  |",
        "| 1 | b | 1   | 1                  |",
        "| 1 | c | 2   | 1                  |",
        "| 2 | d | 3   | 1                  |",
        "| 2 | e | 4   | 2                  |",
        "| 2 | f | 5   | 1                  |",
        "| 2 | g | 6   | 1                  |",
        "| 2 | h | 6   | 0                  |",
        "| 2 | i | 6   | 0                  |",
        "| 2 | j | 6   | 0                  |",
        "+---+---+-----+--------------------+",
    ];
    assert_batches_eq!(
        expected,
        &execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap()
    );
    // Evaluate and update_state is called for each input row
    assert_eq!(test_state.evaluate_called(), 10);
    assert_eq!(test_state.update_state_called(), 10);
    assert_eq!(test_state.evaluate_all_called(), 0);
}

/// user defined window function using rank
#[tokio::test]
async fn test_udwf_query_include_rank() {
    let test_state = TestState::new().with_include_rank();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let expected = vec![
        "+---+---+-----+--------------------+",
        "| x | y | val | odd_counter(t.val) |",
        "+---+---+-----+--------------------+",
        "| 1 | a | 0   | 3                  |",
        "| 1 | b | 1   | 2                  |",
        "| 1 | c | 2   | 1                  |",
        "| 2 | d | 3   | 7                  |",
        "| 2 | e | 4   | 6                  |",
        "| 2 | f | 5   | 5                  |",
        "| 2 | g | 6   | 4                  |",
        "| 2 | h | 6   | 3                  |",
        "| 2 | i | 6   | 2                  |",
        "| 2 | j | 6   | 1                  |",
        "+---+---+-----+--------------------+",
    ];
    assert_batches_eq!(
        expected,
        &execute(&ctx, UNBOUNDED_WINDOW_QUERY).await.unwrap()
    );
    assert_eq!(test_state.evaluate_called(), 0);
    assert_eq!(test_state.evaluate_all_called(), 0);
    // evaluated on 2 distinct batches (when x=1 and x=2)
    assert_eq!(test_state.evaluate_all_with_rank_called(), 2);
}

/// user defined window function with bounded window using rank
#[tokio::test]
async fn test_udwf_bounded_query_include_rank() {
    let test_state = TestState::new().with_include_rank();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let expected = vec![
        "+---+---+-----+--------------------+",
        "| x | y | val | odd_counter(t.val) |",
        "+---+---+-----+--------------------+",
        "| 1 | a | 0   | 3                  |",
        "| 1 | b | 1   | 2                  |",
        "| 1 | c | 2   | 1                  |",
        "| 2 | d | 3   | 7                  |",
        "| 2 | e | 4   | 6                  |",
        "| 2 | f | 5   | 5                  |",
        "| 2 | g | 6   | 4                  |",
        "| 2 | h | 6   | 3                  |",
        "| 2 | i | 6   | 2                  |",
        "| 2 | j | 6   | 1                  |",
        "+---+---+-----+--------------------+",
    ];
    assert_batches_eq!(
        expected,
        &execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap()
    );
    assert_eq!(test_state.evaluate_called(), 0);
    assert_eq!(test_state.evaluate_all_called(), 0);
    // evaluated on 2 distinct batches (when x=1 and x=2)
    assert_eq!(test_state.evaluate_all_with_rank_called(), 2);
}

async fn execute(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Returns an context with a table "t" and the "first" and "time_sum"
/// aggregate functions registered.
///
/// "t" contains this data:
///
/// ```text
/// x | y | val
/// 1 | a | 0
/// 1 | b | 1
/// 1 | c | 2
/// 2 | d | 3
/// 2 | e | 4
/// 2 | f | 5
/// 2 | g | 6
/// 2 | h | 6
/// 2 | i | 6
/// 2 | j | 6
/// ```
struct TestContext {
    ctx: SessionContext,
    test_state: Arc<TestState>,
}

impl TestContext {
    fn new(test_state: TestState) -> Self {
        let test_state = Arc::new(test_state);
        let x = Int64Array::from(vec![1, 1, 1, 2, 2, 2, 2, 2, 2, 2]);
        let y = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]);
        let val = Int64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 6, 6, 6]);

        let batch = RecordBatch::try_from_iter(vec![
            ("x", Arc::new(x) as _),
            ("y", Arc::new(y) as _),
            ("val", Arc::new(val) as _),
        ])
        .unwrap();

        let mut ctx = SessionContext::new();

        ctx.register_batch("t", batch).unwrap();

        // Tell DataFusion about the window function
        OddCounter::register(&mut ctx, Arc::clone(&test_state));

        Self { ctx, test_state }
    }
}

#[derive(Debug, Default)]
struct TestState {
    /// How many times was `evaluate_all` called?
    evaluate_all_called: AtomicUsize,
    /// How many times was `evaluate` called?
    evaluate_called: AtomicUsize,
    /// How many times was `update_state` called?
    update_state_called: AtomicUsize,
    /// How many times was `evaluate_all_with_rank` called?
    evaluate_all_with_rank_called: AtomicUsize,
    /// should the functions say they use the window frame?
    uses_window_frame: bool,
    /// should the functions say they support bounded execution
    supports_bounded_execution: bool,
    /// should the functions they need include rank
    include_rank: bool,
}

impl TestState {
    fn new() -> Self {
        Default::default()
    }

    /// Set that this function should use the window frame
    fn with_uses_window_frame(mut self) -> Self {
        self.uses_window_frame = true;
        self
    }

    /// Set that this function should use bounded / stateful execution
    fn with_supports_bounded_execution(mut self) -> Self {
        self.supports_bounded_execution = true;
        self
    }

    /// Set that this function should include rank
    fn with_include_rank(mut self) -> Self {
        self.include_rank = true;
        self
    }

    /// return the evaluate_all_called counter
    fn evaluate_all_called(&self) -> usize {
        self.evaluate_all_called.load(Ordering::SeqCst)
    }

    /// update the evaluate_all_called counter
    fn inc_evaluate_all_called(&self) {
        self.evaluate_all_called.fetch_add(1, Ordering::SeqCst);
    }

    /// return the evaluate_called counter
    fn evaluate_called(&self) -> usize {
        self.evaluate_called.load(Ordering::SeqCst)
    }

    /// update the evaluate_called counter
    fn inc_evaluate_called(&self) {
        self.evaluate_called.fetch_add(1, Ordering::SeqCst);
    }

    /// return the update_state_called counter
    fn update_state_called(&self) -> usize {
        self.update_state_called.load(Ordering::SeqCst)
    }

    /// update the update_state_called counter
    fn inc_update_state_called(&self) {
        self.update_state_called.fetch_add(1, Ordering::SeqCst);
    }

    /// return the evaluate_all_with_rank_called counter
    fn evaluate_all_with_rank_called(&self) -> usize {
        self.evaluate_all_with_rank_called.load(Ordering::SeqCst)
    }

    /// update the evaluate_all_with_rank_called counter
    fn inc_evaluate_all_with_rank_called(&self) {
        self.evaluate_all_with_rank_called
            .fetch_add(1, Ordering::SeqCst);
    }
}

// Partition Evaluator that counts the number of odd numbers in the window frame using evaluate
#[derive(Debug)]
struct OddCounter {
    test_state: Arc<TestState>,
}

impl OddCounter {
    fn new(test_state: Arc<TestState>) -> Self {
        Self { test_state }
    }

    fn register(ctx: &mut SessionContext, test_state: Arc<TestState>) {
        let name = "odd_counter";
        let volatility = Volatility::Immutable;

        let signature = Signature::exact(vec![DataType::Int64], volatility);

        let return_type = Arc::new(DataType::Int64);
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::clone(&return_type)));

        let partition_evaluator_factory: PartitionEvaluatorFactory =
            Arc::new(move || Ok(Box::new(OddCounter::new(Arc::clone(&test_state)))));

        ctx.register_udwf(WindowUDF::new(
            name,
            &signature,
            &return_type,
            &partition_evaluator_factory,
        ))
    }
}

impl PartitionEvaluator for OddCounter {
    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        println!("evaluate, values: {values:#?}, range: {range:?}");

        self.test_state.inc_evaluate_called();
        let values: &Int64Array = values.get(0).unwrap().as_primitive();
        let values = values.slice(range.start, range.len());
        let scalar = ScalarValue::Int64(Some(odd_count(&values)));
        Ok(scalar)
    }

    fn evaluate_all(
        &mut self,
        values: &[arrow_array::ArrayRef],
        num_rows: usize,
    ) -> Result<arrow_array::ArrayRef> {
        println!("evaluate_all, values: {values:#?}, num_rows: {num_rows}");

        self.test_state.inc_evaluate_all_called();
        Ok(odd_count_arr(
            values.get(0).unwrap().as_primitive(),
            num_rows,
        ))
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        self.test_state.inc_evaluate_all_with_rank_called();
        println!("evaluate_all_with_rank, values: {num_rows:#?}, ranks_in_partitions: {ranks_in_partition:?}");
        // when evaluating with ranks, just return the inverse rank instead
        let array: Int64Array = ranks_in_partition
            .iter()
            // cloned range is an iterator
            .cloned()
            .flatten()
            .map(|v| (num_rows - v) as i64)
            .collect();
        Ok(Arc::new(array))
    }

    fn update_state(
        &mut self,
        _state: &WindowAggState,
        _idx: usize,
        _range_columns: &[ArrayRef],
        _sort_partition_points: &[Range<usize>],
    ) -> Result<()> {
        self.test_state.inc_update_state_called();
        Ok(())
    }

    fn supports_bounded_execution(&self) -> bool {
        self.test_state.supports_bounded_execution
    }

    fn uses_window_frame(&self) -> bool {
        self.test_state.uses_window_frame
    }

    fn include_rank(&self) -> bool {
        self.test_state.include_rank
    }
}

/// returns the number of entries in arr that are odd
fn odd_count(arr: &Int64Array) -> i64 {
    arr.iter().filter_map(|x| x.map(|x| x % 2)).sum()
}

/// returns an array of num_rows that has the number of odd values in `arr`
fn odd_count_arr(arr: &Int64Array, num_rows: usize) -> arrow_array::ArrayRef {
    let array: Int64Array = std::iter::repeat(odd_count(arr)).take(num_rows).collect();
    Arc::new(array)
}
