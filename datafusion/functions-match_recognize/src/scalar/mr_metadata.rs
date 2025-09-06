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

//! Scalar UDFs used by MATCH_RECOGNIZE to expose metadata columns
//!
//! These functions are pass-through helpers that take a single argument
//! (the respective virtual metadata column) and return it unchanged.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{plan_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

/// Generate a nullary Scalar UDF implementation for a MATCH_RECOGNIZE
/// metadata function.
///
/// This macro defines a struct type and implements [`ScalarUDFImpl`] for it
/// following the common pattern used by all MATCH_RECOGNIZE metadata UDFs
/// (i.e. `match_number`, `match_sequence_number`, `classifier`).
///
/// The generated UDF:
/// - has a nullary signature (no SQL-visible arguments)
/// - advertises a fixed Arrow return [`DataType`]
/// - is immutable (`Volatility::Immutable`)
/// - errors when invoked directly (these functions are only valid within
///   `MATCH_RECOGNIZE MEASURES` and are rewritten to use virtual columns)
/// - returns a non-nullable field in `return_field_from_args`
///
/// Parameters:
/// - `struct_name`: The Rust type name for the UDF implementation struct
/// - `fn_name`: The SQL-visible function name (e.g. "match_number")
/// - `return_dt`: The Arrow return `DataType` (e.g. `DataType::UInt64`)
macro_rules! define_mr_metadata_udf {
    ($struct_name:ident, $fn_name:expr, $return_dt:expr, $doc_desc:expr, $doc_syntax:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        #[user_doc(
            doc_section(label = "Other Functions"),
            description = $doc_desc,
            syntax_example = $doc_syntax
        )]
        pub struct $struct_name {
            signature: Signature,
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $struct_name {
            pub fn new() -> Self {
                // Nullary function
                Self {
                    signature: Signature::one_of(
                        vec![datafusion_expr::TypeSignature::Exact(vec![])],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                $fn_name
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
                Ok(Arc::new(Field::new(self.name(), $return_dt, false)))
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_dt)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                if !args.args.is_empty() {
                    return plan_err!(concat!($fn_name, "() does not take arguments"));
                }
                plan_err!(concat!(
                    $fn_name,
                    "() is only valid within MATCH_RECOGNIZE MEASURES and will be rewritten to a virtual column"
                ))
            }

            fn documentation(&self) -> Option<&Documentation> {
                self.doc()
            }
        }
    };
}

define_mr_metadata_udf!(
    MatchNumberUdf,
    "match_number",
    DataType::UInt64,
    "Returns the match number of the current row within MATCH_RECOGNIZE.",
    "match_number()"
);
define_mr_metadata_udf!(
    MatchSequenceNumberUdf,
    "match_sequence_number",
    DataType::UInt64,
    "Returns the sequence number of the current row within the current match.",
    "match_sequence_number()"
);
define_mr_metadata_udf!(
    ClassifierUdf,
    "classifier",
    DataType::Utf8,
    "Returns the symbol (classifier) for the current row within the pattern match.",
    "classifier()"
);

pub fn match_number() -> ScalarUDF {
    ScalarUDF::from(MatchNumberUdf::new())
}
pub fn match_sequence_number() -> ScalarUDF {
    ScalarUDF::from(MatchSequenceNumberUdf::new())
}
pub fn classifier() -> ScalarUDF {
    ScalarUDF::from(ClassifierUdf::new())
}
