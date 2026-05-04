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

//! Logical [`Sample`] extension node, the lowering target for
//! `TABLESAMPLE` (SQL) and friends. This is intentionally an
//! [`UserDefinedLogicalNodeCore`] rather than a first-class
//! [`LogicalPlan`] variant — promoting it to a built-in variant
//! is a follow-up touching every visitor / serializer / optimizer
//! that match-arms `LogicalPlan`.
//!
//! Issue: <https://github.com/apache/datafusion/issues/16533>

use std::cmp::Ordering;
use std::fmt::{self, Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::{DFSchemaRef, plan_err};

use crate::logical_plan::extension::UserDefinedLogicalNodeCore;
use crate::{Expr, LogicalPlan};

/// SQL TABLESAMPLE method.
///
/// Currently only [`SampleMethod::System`] is supported end-to-end.
/// `BERNOULLI` parses but is rejected at planning time until a
/// generic post-scan filter implementation lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SampleMethod {
    /// Block-level sampling (Postgres `SYSTEM`, Hive `BLOCK`).
    /// Implemented as a hierarchical hybrid across files, row
    /// groups, and rows so the IO win at small fractions doesn't
    /// concentrate at a single granularity.
    System,
}

impl Display for SampleMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SampleMethod::System => write!(f, "SYSTEM"),
        }
    }
}

/// Logical extension node representing a TABLESAMPLE clause.
///
/// `fraction` is in `(0.0, 1.0]`. `seed` is the optional `REPEATABLE`
/// seed; selection is deterministic when seeded.
///
/// Schema is the same as the input — sampling drops rows but never
/// changes the column shape.
#[derive(Debug, Clone)]
pub struct Sample {
    pub input: Arc<LogicalPlan>,
    pub method: SampleMethod,
    pub fraction: f64,
    pub seed: Option<u64>,
    schema: DFSchemaRef,
}

impl Sample {
    /// Construct a [`Sample`] over `input`. Validates that
    /// `fraction` is in `(0.0, 1.0]`.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        method: SampleMethod,
        fraction: f64,
        seed: Option<u64>,
    ) -> datafusion_common::Result<Self> {
        if !fraction.is_finite() || fraction <= 0.0 || fraction > 1.0 {
            return plan_err!(
                "TABLESAMPLE fraction must be in (0.0, 1.0]; got {fraction}"
            );
        }
        let schema = Arc::clone(input.schema());
        Ok(Self {
            input,
            method,
            fraction,
            seed,
            schema,
        })
    }
}

// `UserDefinedLogicalNodeCore` requires PartialEq + Eq + Hash + PartialOrd.
// Floats don't implement Eq/Hash/Ord directly; we hash + compare on the
// bit pattern of `fraction`, which is consistent with the way DataFusion
// hashes float ScalarValues elsewhere.

impl PartialEq for Sample {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.input, &other.input)
            && self.method == other.method
            && self.fraction.to_bits() == other.fraction.to_bits()
            && self.seed == other.seed
    }
}

impl Eq for Sample {}

impl Hash for Sample {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use Arc address for input; downstream is opaque.
        (Arc::as_ptr(&self.input) as usize).hash(state);
        self.method.hash(state);
        self.fraction.to_bits().hash(state);
        self.seed.hash(state);
    }
}

impl PartialOrd for Sample {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Order by (method, fraction-bits, seed). Inputs aren't
        // ordered (LogicalPlan doesn't impl Ord); ignore for ordering.
        Some(
            self.method
                .cmp(&other.method)
                .then(self.fraction.to_bits().cmp(&other.fraction.to_bits()))
                .then(self.seed.cmp(&other.seed)),
        )
    }
}

impl UserDefinedLogicalNodeCore for Sample {
    fn name(&self) -> &str {
        "Sample"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Sample: {method}({pct:.4}%)",
            method = self.method,
            pct = self.fraction * 100.0,
        )?;
        if let Some(seed) = self.seed {
            write!(f, " REPEATABLE({seed})")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("Sample takes no expressions; got {}", exprs.len());
        }
        if inputs.len() != 1 {
            return plan_err!("Sample takes exactly one input; got {}", inputs.len());
        }
        let input = Arc::new(inputs.swap_remove(0));
        Self::try_new(input, self.method, self.fraction, self.seed)
    }

    fn supports_limit_pushdown(&self) -> bool {
        // LIMIT(SAMPLE(x)) ≠ SAMPLE(LIMIT(x)) — pushing limit below
        // a sample changes the population we sample from.
        false
    }
}

/// Convenience: wrap a `LogicalPlan` in a `Sample` extension node.
pub fn sample_plan(
    input: Arc<LogicalPlan>,
    method: SampleMethod,
    fraction: f64,
    seed: Option<u64>,
) -> datafusion_common::Result<LogicalPlan> {
    let sample = Sample::try_new(input, method, fraction, seed)?;
    Ok(LogicalPlan::Extension(crate::logical_plan::Extension {
        node: Arc::new(sample),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::{EmptyRelation, LogicalPlan};
    use datafusion_common::DFSchema;

    fn empty_plan() -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }))
    }

    /// `fmt::Formatter::new` is nightly-only; route through Display.
    struct Explain<'a>(&'a Sample);
    impl Display for Explain<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_for_explain(f)
        }
    }
    fn explain(s: &Sample) -> String {
        format!("{}", Explain(s))
    }

    #[test]
    fn sample_validates_fraction_range() {
        let p = empty_plan();
        assert!(
            Sample::try_new(Arc::clone(&p), SampleMethod::System, 0.0, None).is_err()
        );
        assert!(
            Sample::try_new(Arc::clone(&p), SampleMethod::System, 1.5, None).is_err()
        );
        assert!(
            Sample::try_new(Arc::clone(&p), SampleMethod::System, f64::NAN, None)
                .is_err()
        );
        assert!(Sample::try_new(Arc::clone(&p), SampleMethod::System, 0.5, None).is_ok());
        assert!(Sample::try_new(Arc::clone(&p), SampleMethod::System, 1.0, None).is_ok());
    }

    #[test]
    fn sample_explain_format() {
        let p = empty_plan();
        let s = Sample::try_new(p, SampleMethod::System, 0.1, Some(42)).unwrap();
        assert_eq!(explain(&s), "Sample: SYSTEM(10.0000%) REPEATABLE(42)");
    }

    #[test]
    fn sample_explain_format_no_seed() {
        let p = empty_plan();
        let s = Sample::try_new(p, SampleMethod::System, 0.05, None).unwrap();
        assert_eq!(explain(&s), "Sample: SYSTEM(5.0000%)");
    }

    #[test]
    fn sample_with_exprs_and_inputs_rebuilds() {
        let p = empty_plan();
        let s =
            Sample::try_new(Arc::clone(&p), SampleMethod::System, 0.1, Some(7)).unwrap();
        let rebuilt = s.with_exprs_and_inputs(vec![], vec![(*p).clone()]).unwrap();
        assert_eq!(rebuilt.method, SampleMethod::System);
        assert_eq!(rebuilt.fraction, 0.1);
        assert_eq!(rebuilt.seed, Some(7));
    }
}
