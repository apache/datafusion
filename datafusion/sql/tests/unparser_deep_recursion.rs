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

//! The unparser raises `recursive`'s minimum stack size (a process-global) for the
//! duration of a deep unparse. Run alongside the crate's other tests, this regression
//! lives in its own test binary: those tests churn that global as their `StackGuard`s
//! drop, and a value restored on another thread mid-unparse would drop the red zone
//! below what the deep recursion needs and overflow. Alone in its own process, nothing
//! else touches the red zone while the deep unparse runs.

#![cfg(feature = "recursive_protection")]

use datafusion_expr::{Expr, col, lit};
use datafusion_functions_nested::expr_fn::array_has;
use datafusion_sql::unparser::Unparser;
use datafusion_sql::unparser::dialect::PostgreSqlDialect;

/// Regression test for https://github.com/apache/datafusion/issues/23056
///
/// Deeply-nested expressions whose unparse path routes through scalar function
/// arguments and dialect scalar-function overrides used to overflow the OS stack even
/// with `recursive_protection` enabled, because the per-level stack cost of those paths
/// exceeds the default `recursive` red zone and the unparser installed no `StackGuard`.
#[test]
fn deeply_nested_expr_does_not_overflow_stack() {
    // Far deeper than the ~60 levels that overflow without protection, but bounded so
    // the trampoline's heap stacks stay reasonable in debug.
    const DEPTH: usize = 2_000;

    // Run on an explicit, realistically-sized thread stack. The work is performed on a
    // spawned thread so an overflow (in the unfixed code) aborts the process and fails
    // the test deterministically rather than depending on the harness thread's stack.
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            // 1. Linear chain through a dialect scalar-function override:
            //    array_has(array_has(... array_has(col, 'x') ...), 'x').
            //    PostgreSqlDialect unparses array_has via array_has_to_sql_any, which
            //    recurses back into the unparser for each argument.
            let mut nested_fn: Expr = col("c");
            for _ in 0..DEPTH {
                nested_fn = array_has(nested_fn, lit("x"));
            }
            let pg = PostgreSqlDialect {};
            Unparser::new(&pg)
                .expr_to_sql(&nested_fn)
                .expect("deeply nested scalar function should unparse");

            // 2. Linear chain of plain binary operators, exercising the inner -> inner
            //    recursion on the default dialect.
            let mut nested_binary: Expr = col("c");
            for _ in 0..DEPTH {
                nested_binary = nested_binary + lit(1);
            }
            Unparser::default()
                .expr_to_sql(&nested_binary)
                .expect("deeply nested binary expression should unparse");

            // 3. Same binary chain in pretty mode. Pretty mode runs
            //    `remove_unnecessary_nesting` at every level, which recurses alongside
            //    the unparse itself; this locks down that second recursion site.
            Unparser::default()
                .with_pretty(true)
                .expr_to_sql(&nested_binary)
                .expect("deeply nested binary expression should unparse in pretty mode");
        })
        .unwrap();

    // If the unparser overflows, the process aborts and this join is never reached;
    // otherwise the spawned thread returns cleanly.
    handle.join().expect("unparsing thread should not panic");
}
