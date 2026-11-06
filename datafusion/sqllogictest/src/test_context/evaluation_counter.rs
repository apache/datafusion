// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Functions that count how many rows a call is evaluated for, so a test can
//! check that the optimizer does not evaluate a call more (or fewer) times
//! than the query says:
//!
//! - `counted_volatile(x)` returns `x` and is volatile, like `random()`.
//! - `counted_immutable(x)` returns `x` and is immutable.
//! - `evaluations()` returns the number of rows that the two functions above
//!   were evaluated for since the last call to `evaluations()`, and resets it.
//!
//! The count is per row, not per call: a function is invoked once per batch,
//! and `CASE` evaluates its branches on a subset of a batch.
//!
//! ```text
//! statement ok
//! SELECT k FROM (SELECT counted_volatile(a) AS k FROM t) WHERE k > 1;
//!
//! query I
//! SELECT evaluations();
//! ----
//! 3
//! ```

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Registers `counted_volatile`, `counted_immutable` and `evaluations` with `ctx`. Each
/// context gets its own counter.
pub fn register_evaluation_counter(ctx: &SessionContext) {
    let counter = Arc::new(AtomicUsize::new(0));
    for (name, volatility) in [
        ("counted_volatile", Volatility::Volatile),
        ("counted_immutable", Volatility::Immutable),
    ] {
        ctx.register_udf(ScalarUDF::new_from_impl(Counted {
            name,
            signature: Signature::any(1, volatility),
            counter: Arc::clone(&counter),
        }));
    }
    ctx.register_udf(ScalarUDF::new_from_impl(Evaluations {
        signature: Signature::nullary(Volatility::Volatile),
        counter,
    }));
}

/// Returns its argument and adds the number of rows to `counter`.
#[derive(Debug)]
struct Counted {
    name: &'static str,
    signature: Signature,
    counter: Arc<AtomicUsize>,
}

impl ScalarUDFImpl for Counted {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.counter.fetch_add(args.number_rows, Ordering::SeqCst);
        Ok(args.args.swap_remove(0))
    }
}

/// Returns the count and resets it.
#[derive(Debug)]
struct Evaluations {
    signature: Signature,
    counter: Arc<AtomicUsize>,
}

impl ScalarUDFImpl for Evaluations {
    fn name(&self) -> &str {
        "evaluations"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let count = self.counter.swap(0, Ordering::SeqCst);
        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
            count as i64,
        ))))
    }
}

// Two instances are equal only if they share a counter.

impl PartialEq for Counted {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && Arc::ptr_eq(&self.counter, &other.counter)
    }
}

impl Eq for Counted {}

impl Hash for Counted {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        Arc::as_ptr(&self.counter).hash(state);
    }
}

impl PartialEq for Evaluations {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.counter, &other.counter)
    }
}

impl Eq for Evaluations {}

impl Hash for Evaluations {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.counter).hash(state);
    }
}
