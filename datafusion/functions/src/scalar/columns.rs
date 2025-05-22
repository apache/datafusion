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

//! Implementation for the COLUMNS() function used in SQL macros for dynamic column selection
//!
//! NOTE: This UDF serves as a marker for the MacroExpander to replace with actual column names
//! during SQL macro expansion. When called directly outside of SQL macros, it returns an empty
//! result and logs a warning.

use arrow::array::builder::{ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// The COLUMNS function is specifically designed to be used exclusively within SQL macros
/// for dynamic column selection. It serves as a special marker that is processed by the
/// MacroExpander component during query planning.
///
/// # Architecture
/// COLUMNS is a special function in DataFusion with a specific architectural role:
///
/// 1. **Intended Use - In SQL macros**: When used within `${}` substitution in a SQL macro, the
///    MacroExpander component identifies this function call during query planning and replaces it
///    with the actual column names that match the pattern from the referenced table's schema.
///
/// 2. **Direct Use (Not Recommended)**: When called directly outside of SQL macros, this function
///    returns an empty list and logs a warning. Direct calls to COLUMNS() should be replaced with
///    queries against information_schema.columns.
///
/// # Pattern Matching (in SQL Macros)
/// When used in SQL macros, the pattern supports:
/// - `*` - Matches any sequence of characters
/// - `?` - Matches any single character
///
/// # Examples
///
/// **Correct Usage (Within SQL Macros):**
/// ```sql
/// CREATE MACRO select_all AS
///   SELECT ${COLUMNS('*')}
///   FROM ${table}
/// ```
///
/// **Alternative for Direct Use:**
/// Instead of:
/// ```sql
/// SELECT COLUMNS('user*') -- Don't do this
/// ```
///
/// Use information_schema:
/// ```sql
/// SELECT column_name FROM information_schema.columns
/// WHERE table_name = 'users' AND column_name LIKE 'user%'
/// ```
pub fn register_columns_function() -> ScalarUDF {
    #[derive(Debug)]
    struct ColumnsUdf {
        signature: Signature,
    }

    impl ScalarUDFImpl for ColumnsUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "COLUMNS"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))))
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let pattern_arg = match args.args.first() {
                Some(arg) => arg,
                None => {
                    return Err(datafusion_common::error::DataFusionError::Internal(
                        "COLUMNS function expects a string pattern as first argument"
                            .to_string(),
                    ))
                }
            };

            let pattern = match pattern_arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(pattern))) => {
                    pattern.clone()
                }
                _ => {
                    return Err(datafusion_common::error::DataFusionError::Internal(
                        "COLUMNS function expects a string pattern as first argument"
                            .to_string(),
                    ))
                }
            };

            eprintln!(
                "Warning: COLUMNS('{}') was called directly as a function. This function is designed \n\
                 exclusively for use within SQL macros and is not meant to be called directly.\n\n\
                 Alternative: To get column information, use a query like:\n\
                 SELECT column_name FROM information_schema.columns\n\
                 WHERE table_name = 'your_table' AND column_name LIKE '{}'", 
                pattern, pattern.replace('*', "%").replace('?', "_")
            );

            let mut list_builder = ListBuilder::new(StringBuilder::new());
            list_builder.append(true);
            let list_array = list_builder.finish();
            let list_scalar = ScalarValue::List(Arc::new(list_array));

            Ok(ColumnarValue::Scalar(list_scalar))
        }
    }

    let columns_udf = ColumnsUdf {
        signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Stable),
    };

    ScalarUDF::new_from_impl(columns_udf)
}

/// Evaluates whether a column should be included based on lists of included/excluded columns
pub fn evaluate_column_filter(
    column_name: &str,
    include_cols: &[String],
    exclude_cols: &[String],
) -> bool {
    let include =
        include_cols.is_empty() || include_cols.contains(&column_name.to_string());

    let exclude =
        !exclude_cols.is_empty() && exclude_cols.contains(&column_name.to_string());

    include && !exclude
}
