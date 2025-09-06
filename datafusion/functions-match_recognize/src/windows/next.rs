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

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::ScalarValue;
use datafusion_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;
use datafusion_expr::{
    Documentation, Literal, PartitionEvaluator, Signature, TypeSignature, Volatility,
    WindowUDFImpl,
};
use datafusion_functions_window::get_or_init_udwf;
use datafusion_functions_window::utils::{
    get_scalar_value_from_args, get_signed_integer,
};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use std::sync::LazyLock;

use super::common::{bits_col_expr_from_expr, MRShiftKind, MatchRecognizeShiftEvaluator};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct MatchRecognizeNext {
    signature: Signature,
}

impl Default for MatchRecognizeNext {
    fn default() -> Self {
        Self::new()
    }
}

impl MatchRecognizeNext {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(3), TypeSignature::Any(4)],
                Volatility::Immutable,
            ),
        }
    }
}

static MR_NEXT_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns a value from a following row in a MATCH_RECOGNIZE pattern match. This function can only be used within MATCH_RECOGNIZE clauses.",
        "next(expression[, offset[, default[, classifier_mask]]])",
    )
    .with_argument("expression", "Expression to operate on")
    .with_argument("offset", "Non-negative integer. Rows ahead to look. Defaults to 1.")
    .with_argument("default", "Default value if offset is out of bounds")
    .with_argument("classifier_mask", "Optional Boolean. Restrict to rows matching the symbol.")
    .build()
});

impl WindowUDFImpl for MatchRecognizeNext {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "next"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
        // New calling convention: next(value, [offset, [default, [classifier_mask]]])
        // Mask is always last if present.
        let mask_provided = partition_evaluator_args.input_exprs().len() == 4;
        let (offset_idx, default_idx) = (1, 2);

        let shift_offset = match get_scalar_value_from_args(
            partition_evaluator_args.input_exprs(),
            offset_idx,
        )? {
            None => 1i64,
            Some(v) if v.is_null() => 1i64,
            Some(v) => get_signed_integer(v)?,
        };
        if shift_offset < 0 {
            return datafusion_common::exec_err!("Offset for NEXT must be non-negative");
        }
        let shift_offset = shift_offset as usize;
        let default_value = get_scalar_value_from_args(
            partition_evaluator_args.input_exprs(),
            default_idx,
        )?
        .unwrap_or(ScalarValue::Null);
        Ok(Box::new(MatchRecognizeShiftEvaluator::new(
            mask_provided,
            shift_offset,
            default_value,
            MRShiftKind::Next,
        )))
    }

    fn field(
        &self,
        field_args: WindowUDFFieldArgs,
    ) -> datafusion_common::Result<FieldRef> {
        let return_type = field_args
            .input_fields()
            .first()
            .map(|f| f.data_type())
            .cloned()
            .unwrap_or(DataType::Null);
        Ok(Field::new(field_args.name(), return_type, true).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MR_NEXT_DOCUMENTATION)
    }
}

get_or_init_udwf!(
    MatchRecognizeNext,
    mr_next,
    "Returns the next value in a MATCH_RECOGNIZE pattern",
    MatchRecognizeNext::new
);

pub fn mr_next(
    arg: datafusion_expr::Expr,
    offset: Option<i64>,
    default: Option<ScalarValue>,
) -> datafusion_common::Result<datafusion_expr::Expr> {
    let offset_lit = offset.map(|v| v.lit()).unwrap_or(ScalarValue::Null.lit());
    let default_lit = default.unwrap_or(ScalarValue::Null).lit();
    Ok(match bits_col_expr_from_expr(&arg)? {
        Some(mask) => mr_next_udwf().call(vec![arg, offset_lit, default_lit, mask]),
        None => mr_next_udwf().call(vec![arg, offset_lit, default_lit]),
    })
}
