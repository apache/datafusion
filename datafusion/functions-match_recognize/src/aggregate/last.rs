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

use std::any::Any;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow::datatypes::{DataType, Field, FieldRef};

use datafusion_common::{plan_err, Result};
use datafusion_expr::aggregate_doc_sections::DOC_SECTION_GENERAL;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::Documentation;
use datafusion_expr::Expr;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, ReversedUDAF, Signature, TypeSignature, Volatility,
};
use datafusion_functions_aggregate::create_func;

use crate::aggregate::utils::bits_col_from_expr;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MatchRecognizeLastAgg {
    signature: Signature,
}

/// Returns the last value within a MATCH_RECOGNIZE match. This aggregate is intended
/// to be used when MATCH_RECOGNIZE measures are rewritten to plain aggregates grouped by
/// (PARTITION BY + __mr_match_number). It behaves similarly to the window function `last`,
/// but in aggregate form.
impl MatchRecognizeLastAgg {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

static MR_LAST_AGG_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_GENERAL,
        "Returns the last value in a MATCH_RECOGNIZE pattern match. This function is used internally when MATCH_RECOGNIZE measures are rewritten to aggregates.",
        "last(expression[, classifier_mask])",
    )
    .with_argument("expression", "Expression to operate on")
    .with_argument("classifier_mask", "Optional Boolean. Restrict to rows matching the symbol.")
    .build()
});

impl AggregateUDFImpl for MatchRecognizeLastAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        if arg_fields.is_empty() {
            return plan_err!("last expects at least one argument");
        }
        let first = Arc::clone(&arg_fields[0]);
        Ok(Arc::new(Field::new(
            self.name(),
            first.data_type().clone(),
            first.is_nullable(),
        )))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        plan_err!(
            "last is only valid within MATCH_RECOGNIZE planning and must be rewritten; found at runtime"
        )
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Reversed(super::first::mr_first_udaf())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MR_LAST_AGG_DOCUMENTATION)
    }
}

create_func!(
    MatchRecognizeLastAgg,
    mr_last_udaf,
    MatchRecognizeLastAgg::new()
);

pub fn mr_last_agg(arg: Expr) -> Result<Expr> {
    match bits_col_from_expr(&arg)? {
        Some(col_expr) => Ok(mr_last_udaf().call(vec![arg, col_expr])),
        None => Ok(mr_last_udaf().call(vec![arg])),
    }
}
