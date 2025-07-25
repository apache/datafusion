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

use arrow::array::{
    record_batch, Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_schema::FieldRef;
use datafusion::common::test_util::batches_to_string;
use datafusion::common::{Result, ScalarValue};
use datafusion::prelude::SessionContext;
use datafusion_common::exec_datafusion_err;
use datafusion_expr::{
    PartitionEvaluator, Signature, TypeSignature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_functions_window_common::{
    expr::ExpressionArgs, field::WindowUDFFieldArgs,
};
use datafusion_physical_expr::{
    expressions::{col, lit},
    PhysicalExpr,
};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{
    any::Any,
    ops::Range,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// A query with a window function evaluated over the entire partition
const UNBOUNDED_WINDOW_QUERY: &str = "SELECT x, y, val, \
     odd_counter(val) OVER (PARTITION BY x ORDER BY y) \
     from t ORDER BY x, y";

const UNBOUNDED_WINDOW_QUERY_WITH_ALIAS: &str = "SELECT x, y, val, \
     odd_counter_alias(val) OVER (PARTITION BY x ORDER BY y) \
     from t ORDER BY x, y";

/// A query with a window function evaluated over a moving window
const BOUNDED_WINDOW_QUERY:  &str  =
    "SELECT x, y, val, \
     odd_counter(val) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
     from t ORDER BY x, y";

#[tokio::test]
async fn test_setup() {
    let test_state = TestState::new();
    let TestContext { ctx, test_state: _ } = TestContext::new(test_state);

    let sql = "SELECT * from t order by x, y";
    let actual = execute(&ctx, sql).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+
         | x | y | val |
         +---+---+-----+
         | 1 | a | 0   |
         | 1 | b | 1   |
         | 1 | c | 2   |
         | 2 | d | 3   |
         | 2 | e | 4   |
         | 2 | f | 5   |
         | 2 | g | 6   |
         | 2 | h | 6   |
         | 2 | i | 6   |
         | 2 | j | 6   |
         +---+---+-----+
         "###);
}

/// Basic user defined window function
#[tokio::test]
async fn test_udwf() {
    let test_state = TestState::new();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let actual = execute(&ctx, UNBOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 1                                                                                                                     |
         | 1 | b | 1   | 1                                                                                                                     |
         | 1 | c | 2   | 1                                                                                                                     |
         | 2 | d | 3   | 2                                                                                                                     |
         | 2 | e | 4   | 2                                                                                                                     |
         | 2 | f | 5   | 2                                                                                                                     |
         | 2 | g | 6   | 2                                                                                                                     |
         | 2 | h | 6   | 2                                                                                                                     |
         | 2 | i | 6   | 2                                                                                                                     |
         | 2 | j | 6   | 2                                                                                                                     |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         "###);

    // evaluated on two distinct batches
    assert_eq!(test_state.evaluate_all_called(), 2);
}

#[tokio::test]
async fn test_deregister_udwf() -> Result<()> {
    let test_state = Arc::new(TestState::new());
    let mut ctx = SessionContext::new();
    OddCounter::register(&mut ctx, Arc::clone(&test_state));

    assert!(ctx.state().window_functions().contains_key("odd_counter"));

    ctx.deregister_udwf("odd_counter");

    assert!(!ctx.state().window_functions().contains_key("odd_counter"));

    Ok(())
}

#[tokio::test]
async fn test_udwf_with_alias() {
    let test_state = TestState::new();
    let TestContext { ctx, .. } = TestContext::new(test_state);

    let actual = execute(&ctx, UNBOUNDED_WINDOW_QUERY_WITH_ALIAS)
        .await
        .unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 1                                                                                                                     |
         | 1 | b | 1   | 1                                                                                                                     |
         | 1 | c | 2   | 1                                                                                                                     |
         | 2 | d | 3   | 2                                                                                                                     |
         | 2 | e | 4   | 2                                                                                                                     |
         | 2 | f | 5   | 2                                                                                                                     |
         | 2 | g | 6   | 2                                                                                                                     |
         | 2 | h | 6   | 2                                                                                                                     |
         | 2 | i | 6   | 2                                                                                                                     |
         | 2 | j | 6   | 2                                                                                                                     |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         "###);
}

/// Basic user defined window function with bounded window
#[tokio::test]
async fn test_udwf_bounded_window_ignores_frame() {
    let test_state = TestState::new();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    // Since the UDWF doesn't say it needs the window frame, the frame is ignored
    let actual = execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 1                                                                                                            |
         | 1 | b | 1   | 1                                                                                                            |
         | 1 | c | 2   | 1                                                                                                            |
         | 2 | d | 3   | 2                                                                                                            |
         | 2 | e | 4   | 2                                                                                                            |
         | 2 | f | 5   | 2                                                                                                            |
         | 2 | g | 6   | 2                                                                                                            |
         | 2 | h | 6   | 2                                                                                                            |
         | 2 | i | 6   | 2                                                                                                            |
         | 2 | j | 6   | 2                                                                                                            |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         "###);

    // evaluated on 2 distinct batches (when x=1 and x=2)
    assert_eq!(test_state.evaluate_called(), 0);
    assert_eq!(test_state.evaluate_all_called(), 2);
}

/// Basic user defined window function with bounded window
#[tokio::test]
async fn test_udwf_bounded_window() {
    let test_state = TestState::new().with_uses_window_frame();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let actual = execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 1                                                                                                            |
         | 1 | b | 1   | 1                                                                                                            |
         | 1 | c | 2   | 1                                                                                                            |
         | 2 | d | 3   | 1                                                                                                            |
         | 2 | e | 4   | 2                                                                                                            |
         | 2 | f | 5   | 1                                                                                                            |
         | 2 | g | 6   | 1                                                                                                            |
         | 2 | h | 6   | 0                                                                                                            |
         | 2 | i | 6   | 0                                                                                                            |
         | 2 | j | 6   | 0                                                                                                            |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         "###);

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

    let actual = execute(&ctx, UNBOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 0                                                                                                                     |
         | 1 | b | 1   | 1                                                                                                                     |
         | 1 | c | 2   | 1                                                                                                                     |
         | 2 | d | 3   | 1                                                                                                                     |
         | 2 | e | 4   | 1                                                                                                                     |
         | 2 | f | 5   | 2                                                                                                                     |
         | 2 | g | 6   | 2                                                                                                                     |
         | 2 | h | 6   | 2                                                                                                                     |
         | 2 | i | 6   | 2                                                                                                                     |
         | 2 | j | 6   | 2                                                                                                                     |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         "###);

    assert_eq!(test_state.evaluate_called(), 10);
    assert_eq!(test_state.evaluate_all_called(), 0);
}

/// Basic stateful user defined window function with bounded window
#[tokio::test]
async fn test_stateful_udwf_bounded_window() {
    let test_state = TestState::new()
        .with_supports_bounded_execution()
        .with_uses_window_frame();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let actual = execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 1                                                                                                            |
         | 1 | b | 1   | 1                                                                                                            |
         | 1 | c | 2   | 1                                                                                                            |
         | 2 | d | 3   | 1                                                                                                            |
         | 2 | e | 4   | 2                                                                                                            |
         | 2 | f | 5   | 1                                                                                                            |
         | 2 | g | 6   | 1                                                                                                            |
         | 2 | h | 6   | 0                                                                                                            |
         | 2 | i | 6   | 0                                                                                                            |
         | 2 | j | 6   | 0                                                                                                            |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         "###);

    // Evaluate and update_state is called for each input row
    assert_eq!(test_state.evaluate_called(), 10);
    assert_eq!(test_state.evaluate_all_called(), 0);
}

/// user defined window function using rank
#[tokio::test]
async fn test_udwf_query_include_rank() {
    let test_state = TestState::new().with_include_rank();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let actual = execute(&ctx, UNBOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 3                                                                                                                     |
         | 1 | b | 1   | 2                                                                                                                     |
         | 1 | c | 2   | 1                                                                                                                     |
         | 2 | d | 3   | 7                                                                                                                     |
         | 2 | e | 4   | 6                                                                                                                     |
         | 2 | f | 5   | 5                                                                                                                     |
         | 2 | g | 6   | 4                                                                                                                     |
         | 2 | h | 6   | 3                                                                                                                     |
         | 2 | i | 6   | 2                                                                                                                     |
         | 2 | j | 6   | 1                                                                                                                     |
         +---+---+-----+-----------------------------------------------------------------------------------------------------------------------+
         "###);

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

    let actual = execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 3                                                                                                            |
         | 1 | b | 1   | 2                                                                                                            |
         | 1 | c | 2   | 1                                                                                                            |
         | 2 | d | 3   | 7                                                                                                            |
         | 2 | e | 4   | 6                                                                                                            |
         | 2 | f | 5   | 5                                                                                                            |
         | 2 | g | 6   | 4                                                                                                            |
         | 2 | h | 6   | 3                                                                                                            |
         | 2 | i | 6   | 2                                                                                                            |
         | 2 | j | 6   | 1                                                                                                            |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         "###);

    assert_eq!(test_state.evaluate_called(), 0);
    assert_eq!(test_state.evaluate_all_called(), 0);
    // evaluated on 2 distinct batches (when x=1 and x=2)
    assert_eq!(test_state.evaluate_all_with_rank_called(), 2);
}

/// Basic user defined window function that can return NULL.
#[tokio::test]
async fn test_udwf_bounded_window_returns_null() {
    let test_state = TestState::new()
        .with_uses_window_frame()
        .with_null_for_zero();
    let TestContext { ctx, test_state } = TestContext::new(test_state);

    let actual = execute(&ctx, BOUNDED_WINDOW_QUERY).await.unwrap();

    insta::assert_snapshot!(batches_to_string(&actual), @r###"
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | x | y | val | odd_counter(t.val) PARTITION BY [t.x] ORDER BY [t.y ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         | 1 | a | 0   | 1                                                                                                            |
         | 1 | b | 1   | 1                                                                                                            |
         | 1 | c | 2   | 1                                                                                                            |
         | 2 | d | 3   | 1                                                                                                            |
         | 2 | e | 4   | 2                                                                                                            |
         | 2 | f | 5   | 1                                                                                                            |
         | 2 | g | 6   | 1                                                                                                            |
         | 2 | h | 6   |                                                                                                              |
         | 2 | i | 6   |                                                                                                              |
         | 2 | j | 6   |                                                                                                              |
         +---+---+-----+--------------------------------------------------------------------------------------------------------------+
         "###);

    // Evaluate is called for each input rows
    assert_eq!(test_state.evaluate_called(), 10);
    assert_eq!(test_state.evaluate_all_called(), 0);
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
    /// How many times was `evaluate_all_with_rank` called?
    evaluate_all_with_rank_called: AtomicUsize,
    /// should the functions say they use the window frame?
    uses_window_frame: bool,
    /// should the functions say they support bounded execution
    supports_bounded_execution: bool,
    /// should the functions they need include rank
    include_rank: bool,
    /// should the functions return NULL for 0s?
    null_for_zero: bool,
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

    // Set that this function should return NULL instead of zero.
    fn with_null_for_zero(mut self) -> Self {
        self.null_for_zero = true;
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
        #[derive(Debug, Clone)]
        struct SimpleWindowUDF {
            signature: Signature,
            test_state: Arc<TestState>,
            aliases: Vec<String>,
        }

        impl SimpleWindowUDF {
            fn new(test_state: Arc<TestState>) -> Self {
                let signature =
                    Signature::exact(vec![DataType::Float64], Volatility::Immutable);
                Self {
                    signature,
                    test_state,
                    aliases: vec!["odd_counter_alias".to_string()],
                }
            }
        }

        impl WindowUDFImpl for SimpleWindowUDF {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                "odd_counter"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn partition_evaluator(
                &self,
                _partition_evaluator_args: PartitionEvaluatorArgs,
            ) -> Result<Box<dyn PartitionEvaluator>> {
                Ok(Box::new(OddCounter::new(Arc::clone(&self.test_state))))
            }

            fn aliases(&self) -> &[String] {
                &self.aliases
            }

            fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
                Ok(Field::new(field_args.name(), DataType::Int64, true).into())
            }

            fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
                let Some(other) = other.as_any().downcast_ref::<Self>() else {
                    return false;
                };
                let Self {
                    signature,
                    test_state,
                    aliases,
                } = self;
                signature == &other.signature
                    && Arc::ptr_eq(test_state, &other.test_state)
                    && aliases == &other.aliases
            }

            fn hash_value(&self) -> u64 {
                let Self {
                    signature,
                    test_state,
                    aliases,
                } = self;
                let mut hasher = DefaultHasher::new();
                signature.hash(&mut hasher);
                Arc::as_ptr(test_state).hash(&mut hasher);
                aliases.hash(&mut hasher);
                hasher.finish()
            }
        }

        ctx.register_udwf(WindowUDF::from(SimpleWindowUDF::new(test_state)))
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
        let values: &Int64Array = values[0].as_primitive();
        let values = values.slice(range.start, range.len());
        let scalar = ScalarValue::Int64(
            match (odd_count(&values), self.test_state.null_for_zero) {
                (0, true) => None,
                (n, _) => Some(n),
            },
        );
        Ok(scalar)
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        println!("evaluate_all, values: {values:#?}, num_rows: {num_rows}");

        self.test_state.inc_evaluate_all_called();
        Ok(odd_count_arr(values[0].as_primitive(), num_rows))
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
fn odd_count_arr(arr: &Int64Array, num_rows: usize) -> ArrayRef {
    let array: Int64Array = std::iter::repeat_n(odd_count(arr), num_rows).collect();
    Arc::new(array)
}

#[derive(Debug)]
struct VariadicWindowUDF {
    signature: Signature,
}

impl VariadicWindowUDF {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(0),
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                    TypeSignature::Any(3),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for VariadicWindowUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "variadic_window_udf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        unimplemented!("unnecessary for testing");
    }

    fn field(&self, _: WindowUDFFieldArgs) -> Result<FieldRef> {
        unimplemented!("unnecessary for testing");
    }
}

#[test]
// Fixes: default implementation of `WindowUDFImpl::expressions`
// returns all input expressions to the user-defined window
// function unmodified.
//
// See: https://github.com/apache/datafusion/pull/13169
fn test_default_expressions() -> Result<()> {
    let udwf = WindowUDF::from(VariadicWindowUDF::new());

    let field_a = Field::new("a", DataType::Int32, false);
    let field_b = Field::new("b", DataType::Float32, false);
    let field_c = Field::new("c", DataType::Boolean, false);
    let schema = Schema::new(vec![field_a, field_b, field_c]);

    let test_cases = vec![
        //
        // Zero arguments
        //
        vec![],
        //
        // Single argument
        //
        vec![col("a", &schema)?],
        vec![lit(1)],
        //
        // Two arguments
        //
        vec![col("a", &schema)?, col("b", &schema)?],
        vec![col("a", &schema)?, lit(2)],
        vec![lit(false), col("a", &schema)?],
        //
        // Three arguments
        //
        vec![col("a", &schema)?, col("b", &schema)?, col("c", &schema)?],
        vec![col("a", &schema)?, col("b", &schema)?, lit(false)],
        vec![col("a", &schema)?, lit(0.5), col("c", &schema)?],
        vec![lit(3), col("b", &schema)?, col("c", &schema)?],
    ];

    for input_exprs in &test_cases {
        let input_fields = input_exprs
            .iter()
            .map(|expr: &Arc<dyn PhysicalExpr>| expr.return_field(&schema).unwrap())
            .collect::<Vec<_>>();
        let expr_args = ExpressionArgs::new(input_exprs, &input_fields);

        let ret_exprs = udwf.expressions(expr_args);

        // Verify same number of input expressions are returned
        assert_eq!(
            input_exprs.len(),
            ret_exprs.len(),
            "\nInput expressions: {input_exprs:?}\nReturned expressions: {ret_exprs:?}"
        );

        // Compares each returned expression with original input expressions
        for (expected, actual) in input_exprs.iter().zip(&ret_exprs) {
            assert_eq!(
                format!("{expected:?}"),
                format!("{actual:?}"),
                "\nInput expressions: {:?}\nReturned expressions: {:?}",
                input_exprs,
                ret_exprs
            );
        }
    }
    Ok(())
}

#[derive(Debug)]
struct MetadataBasedWindowUdf {
    name: String,
    signature: Signature,
    metadata: HashMap<String, String>,
}

impl MetadataBasedWindowUdf {
    fn new(metadata: HashMap<String, String>) -> Self {
        // The name we return must be unique. Otherwise we will not call distinct
        // instances of this UDF. This is a small hack for the unit tests to get unique
        // names, but you could do something more elegant with the metadata.
        let name = format!("metadata_based_udf_{}", metadata.len());
        Self {
            name,
            signature: Signature::exact(vec![DataType::UInt64], Volatility::Immutable),
            metadata,
        }
    }
}

impl WindowUDFImpl for MetadataBasedWindowUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let input_field = partition_evaluator_args
            .input_fields()
            .first()
            .ok_or(exec_datafusion_err!("Expected one argument"))?;

        let double_output = input_field
            .metadata()
            .get("modify_values")
            .map(|v| v == "double_output")
            .unwrap_or(false);

        Ok(Box::new(MetadataBasedPartitionEvaluator { double_output }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::UInt64, true)
            .with_metadata(self.metadata.clone())
            .into())
    }

    fn equals(&self, other: &dyn WindowUDFImpl) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        let Self {
            name,
            signature,
            metadata,
        } = self;
        name == &other.name
            && signature == &other.signature
            && metadata == &other.metadata
    }

    fn hash_value(&self) -> u64 {
        let Self {
            name,
            signature,
            metadata: _, // unhashable
        } = self;
        let mut hasher = DefaultHasher::new();
        std::any::type_name::<Self>().hash(&mut hasher);
        name.hash(&mut hasher);
        signature.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug)]
struct MetadataBasedPartitionEvaluator {
    double_output: bool,
}

impl PartitionEvaluator for MetadataBasedPartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let values = values[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let sum = values.iter().fold(0_u64, |acc, v| acc + v.unwrap_or(0));

        let result = if self.double_output { sum * 2 } else { sum };

        Ok(Arc::new(UInt64Array::from_value(result, num_rows)))
    }
}

#[tokio::test]
async fn test_metadata_based_window_fn() -> Result<()> {
    let data_array = Arc::new(UInt64Array::from(vec![0, 5, 10, 15, 20])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("no_metadata", DataType::UInt64, true),
        Field::new("with_metadata", DataType::UInt64, true).with_metadata(
            [("modify_values".to_string(), "double_output".to_string())]
                .into_iter()
                .collect(),
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::clone(&data_array), Arc::clone(&data_array)],
    )?;

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    let no_output_meta_udf = WindowUDF::from(MetadataBasedWindowUdf::new(HashMap::new()));
    let with_output_meta_udf = WindowUDF::from(MetadataBasedWindowUdf::new(
        [("output_metatype".to_string(), "custom_value".to_string())]
            .into_iter()
            .collect(),
    ));

    let df = df.select(vec![
        no_output_meta_udf
            .call(vec![datafusion_expr::col("no_metadata")])
            .alias("meta_no_in_no_out"),
        no_output_meta_udf
            .call(vec![datafusion_expr::col("with_metadata")])
            .alias("meta_with_in_no_out"),
        with_output_meta_udf
            .call(vec![datafusion_expr::col("no_metadata")])
            .alias("meta_no_in_with_out"),
        with_output_meta_udf
            .call(vec![datafusion_expr::col("with_metadata")])
            .alias("meta_with_in_with_out"),
    ])?;

    let actual = df.collect().await?;

    // To test for output metadata handling, we set the expected values on the result
    // To test for input metadata handling, we check the numbers returned
    let mut output_meta = HashMap::new();
    let _ = output_meta.insert("output_metatype".to_string(), "custom_value".to_string());
    let expected_schema = Schema::new(vec![
        Field::new("meta_no_in_no_out", DataType::UInt64, true),
        Field::new("meta_with_in_no_out", DataType::UInt64, true),
        Field::new("meta_no_in_with_out", DataType::UInt64, true)
            .with_metadata(output_meta.clone()),
        Field::new("meta_with_in_with_out", DataType::UInt64, true)
            .with_metadata(output_meta.clone()),
    ]);

    let expected = record_batch!(
        ("meta_no_in_no_out", UInt64, [50, 50, 50, 50, 50]),
        ("meta_with_in_no_out", UInt64, [100, 100, 100, 100, 100]),
        ("meta_no_in_with_out", UInt64, [50, 50, 50, 50, 50]),
        ("meta_with_in_with_out", UInt64, [100, 100, 100, 100, 100])
    )?
    .with_schema(Arc::new(expected_schema))?;

    assert_eq!(expected, actual[0]);

    Ok(())
}
