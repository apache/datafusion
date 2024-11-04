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

use arrow_array::{ArrayRef, BooleanArray, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::prelude::*;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use datafusion_physical_expr::planner::logical2physical;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::sync::Arc;

/// Tests for `LIKE` with truncated statistics to validate incrementing logic
///
/// Create several 2 row batches and ensure that `LIKE` with the min and max value
/// are correctly pruned even when the "statistics" are trunated.
#[test]
fn test_prune_like_truncated_statistics() {
    // Make 2 row random UTF-8 strings
    let mut rng = thread_rng();
    let statistics = TestPruningStatistics::new(&mut rng, 100);

    // All these exprs are true and thus can NOT be pruned
    let min = &statistics.min;
    let max = &statistics.max;

    // prefixes that are in the range of the actual values (`min` -> `max`)
    let matching_prefixes = vec![
        // prefix match on actual min value: a LIKE '<min>%'
        min.clone(),
        max.clone(),
        // prefix match on a prefixes of the actual max: a LIKE '<max_prefix>%'
        truncate_string(max, 5),
        truncate_string(max, 2),
        truncate_string(max, 1),
        // prefixes that are in NOT in the range of the actual values (`min` -> `max`)
        //
        // Note that prefix matches for prefixes of min are *NOT* in the range of
        // the actual values
        //
        // If the actual minimum value is "Andrew" then a prefix such as "And" actually sorts lower
        // than the actual minimum value.
        truncate_string(min, 5),
        truncate_string(min, 2),
        truncate_string(min, 1),
    ];

    // Check that the container isn't (incorrectly) pruned
    for value in &matching_prefixes {
        statistics.assert_not_pruned(like_prefix(col("a"), value));
    }

    println!("Truncating statistics to 4 characters");
    // now truncate the *statistics* to 4 characters to model truncated Parquet statistics
    let statistics = statistics.with_truncate_length(4);
    for value in &matching_prefixes {
        statistics.assert_not_pruned(like_prefix(col("a"), value));
    }
}

#[derive(Debug)]
struct TestPruningStatistics {
    min: String,
    max: String,
    /// how many characters to truncate the min and max strings to when
    /// reporting statistics
    truncate_length: Option<usize>,
}

impl TestPruningStatistics {
    fn new(mut rng: impl Rng, max_len: usize) -> Self {
        let s1 = random_string(&mut rng, max_len);
        let s2 = random_string(&mut rng, max_len);
        let (min, max) = if s1 < s2 { (s1, s2) } else { (s2, s1) };

        Self {
            min,
            max,
            truncate_length: None,
        }
    }

    // Set the truncate length
    fn with_truncate_length(mut self, truncate_length: usize) -> Self {
        self.truncate_length = Some(truncate_length);
        self
    }

    /// Prune the specified expression that references column "a" using these
    /// statistics and asserts that the container was NOT pruned.
    fn assert_not_pruned(&self, expr: Expr) {
        println!(
            "Pruning expr: {}, min: '{}' max: '{}'",
            expr, self.min, self.max
        );
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let expr = logical2physical(&expr, &schema);
        let p = PruningPredicate::try_new(expr, schema.clone()).unwrap();
        let keep = p.prune(self).unwrap();
        assert_eq!(keep, vec![true]);
    }
}

impl PruningStatistics for TestPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        assert_eq!(column.name(), "a");
        let min = if let Some(truncate_length) = self.truncate_length {
            truncate_string(&self.min, truncate_length)
        } else {
            self.min.clone()
        };
        Some(Arc::new(StringArray::from(vec![min])))
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        assert_eq!(column.name(), "a");
        let max = if let Some(truncate_length) = self.truncate_length {
            truncate_string(&self.max, truncate_length)
        } else {
            self.max.clone()
        };
        Some(Arc::new(StringArray::from(vec![max])))
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

/// Return a string of random characters of length 1..=max_len
fn random_string(mut rng: impl Rng, max_len: usize) -> String {
    // pick characters at random (not just ascii)
    match max_len {
        0 => "".to_string(),
        1 => String::from(rng.gen::<char>()),
        _ => {
            let len = rng.gen_range(1..=max_len);
            rng.sample_iter::<char, _>(rand::distributions::Standard)
                .take(len)
                .map(char::from)
                .collect::<String>()
        }
    }
}

/// return at most the first `len` characters of `s`
fn truncate_string(s: &str, len: usize) -> String {
    s.chars().take(len).collect()
}

/// return a LIKE <prefix> expression:
///
/// <expr> `LIKE '<prefix>%'`
fn like_prefix(expr: Expr, prefix: &str) -> Expr {
    expr.like(lit(format!("{}%", prefix)))
}
