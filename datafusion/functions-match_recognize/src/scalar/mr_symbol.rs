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

//! Internal helper UDF used by MATCH_RECOGNIZE planning to attach a symbol name to an expression.
//!
//! This function is a planning-time marker that must be rewritten during MATCH_RECOGNIZE.
//! It is not executed at runtime. The returned field mirrors the type and nullability of
//! the first argument. The second argument is expected to be a `Utf8` literal carrying the
//! symbol name used in the pattern.

use std::any::Any;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{internal_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, DocSection, Documentation, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MrSymbolUdf {
    signature: Signature,
}

impl Default for MrSymbolUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl MrSymbolUdf {
    pub fn new() -> Self {
        // Accept (Any, Utf8)
        let signature =
            Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for MrSymbolUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "mr_symbol"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 {
            return plan_err!("mr_symbol expects exactly two arguments");
        }
        // Return same type/nullability as the first argument
        let first = Arc::clone(&args.arg_fields[0]);
        Ok(Arc::new(Field::new(
            self.name(),
            first.data_type().clone(),
            first.is_nullable(),
        )))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Should not be called if return_field_from_args is used
        internal_err!(
            "mr_symbol return_type should not be called; use return_field_from_args"
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return plan_err!("mr_symbol expects exactly two arguments");
        }
        // This UDF is an internal planning marker and should never be executed at runtime.
        // The planner rewrites expressions that contain mr_symbol, removing the wrapper.
        plan_err!(
            "mr_symbol is only valid within MATCH_RECOGNIZE planning and must be rewritten; found at runtime"
        )
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MR_SYMBOL_DOCUMENTATION)
    }
}

pub fn mr_symbol() -> ScalarUDF {
    ScalarUDF::from(MrSymbolUdf::new())
}

static MR_SYMBOL_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DocSection {
            include: false,
            label: "Internal",
            description: Some(
                "Internal helper used by MATCH_RECOGNIZE planning; not intended for direct use.",
            ),
        },
        "Attaches a MATCH_RECOGNIZE symbol name to an expression for planning and rewrite.",
        "mr_symbol(expression, 'symbol')",
    )
    .with_argument("expression", "Expression to operate on")
    .with_argument(
        "symbol",
        "Utf8 literal indicating the symbol name used in pattern definitions",
    )
    .build()
});
