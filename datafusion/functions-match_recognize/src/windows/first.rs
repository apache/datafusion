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
use datafusion_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;
use datafusion_expr::{
    Documentation, PartitionEvaluator, Signature, TypeSignature, Volatility,
    WindowUDFImpl,
};
use datafusion_functions_window::get_or_init_udwf;
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use std::sync::LazyLock;

use super::common::{bits_col_expr_from_expr, MREdgeKind, MatchRecognizeEdgeEvaluator};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct MatchRecognizeFirst {
    signature: Signature,
}

impl Default for MatchRecognizeFirst {
    fn default() -> Self {
        Self::new()
    }
}

impl MatchRecognizeFirst {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
        }
    }
}

static MR_FIRST_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns the first value in a MATCH_RECOGNIZE pattern match. This function can only be used within MATCH_RECOGNIZE clauses.",
        "first(expression[, classifier_mask])",
    )
    .with_argument("expression", "Expression to operate on")
    .with_argument("classifier_mask", "Optional Boolean. Restrict to rows matching the symbol.")
    .build()
});

impl WindowUDFImpl for MatchRecognizeFirst {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "first"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
        let mask_provided = partition_evaluator_args.input_exprs().len() >= 2;
        Ok(Box::new(MatchRecognizeEdgeEvaluator::new(
            mask_provided,
            MREdgeKind::First,
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
        Some(&MR_FIRST_DOCUMENTATION)
    }
}

get_or_init_udwf!(
    MatchRecognizeFirst,
    mr_first,
    "Returns the first value in a MATCH_RECOGNIZE pattern",
    MatchRecognizeFirst::new
);

pub fn mr_first(
    arg: datafusion_expr::Expr,
) -> datafusion_common::Result<datafusion_expr::Expr> {
    match bits_col_expr_from_expr(&arg)? {
        Some(mask) => Ok(mr_first_udwf().call(vec![arg, mask])),
        None => Ok(mr_first_udwf().call(vec![arg])),
    }
}
