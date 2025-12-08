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

mod boolean_lookup_table;
mod bytes_like_lookup_table;
mod primitive_lookup_table;

use crate::expressions::Literal;
use crate::expressions::case::CaseBody;
use crate::expressions::case::literal_lookup_table::boolean_lookup_table::BooleanIndexMap;
use crate::expressions::case::literal_lookup_table::bytes_like_lookup_table::BytesLikeIndexMap;
use crate::expressions::case::literal_lookup_table::primitive_lookup_table::PrimitiveIndexMap;
use arrow::array::{Array, ArrayRef, UInt32Array, downcast_primitive};
use arrow::datatypes::DataType;
use datafusion_common::{ScalarValue, arrow_datafusion_err, plan_datafusion_err};
use indexmap::IndexMap;
use std::fmt::Debug;

/// Optimization for CASE expressions with literal WHEN and THEN clauses
///
/// for this form:
/// ```sql
/// CASE <expr_a>
///     WHEN <literal_a> THEN <literal_e>
///     WHEN <literal_b> THEN <literal_f>
///     WHEN <literal_c> THEN <literal_g>
///     WHEN <literal_d> THEN <literal_h>
///     ELSE <optional-fallback_literal>
/// END
/// ```
///
/// # Improvement idea
/// TODO - we should think of unwrapping the `IN` expressions into multiple equality comparisons
/// so it will use this optimization as well, e.g.
/// ```sql
/// -- Before
/// CASE
///     WHEN (<expr_a> = <literal_a>) THEN <literal_e>
///     WHEN (<expr_a> in (<literal_b>, <literal_c>) THEN <literal_f>
///     WHEN (<expr_a> = <literal_d>) THEN <literal_g>
/// ELSE <optional-fallback_literal>
///
/// -- After
/// CASE
///     WHEN (<expr_a> = <literal_a>) THEN <literal_e>
///     WHEN (<expr_a> = <literal_b>) THEN <literal_f>
///     WHEN (<expr_a> = <literal_c>) THEN <literal_g>
///     WHEN (<expr_a> = <literal_d>) THEN <literal_h>
///     ELSE <optional-fallback_literal>
/// END
/// ```
///
#[derive(Debug)]
pub(in super::super) struct LiteralLookupTable {
    /// The lookup table to use for evaluating the CASE expression
    lookup: Box<dyn WhenLiteralIndexMap>,

    else_index: u32,

    /// [`ArrayRef`] where `array[i] = then_literals[i]`
    /// the last value in the array is the else_expr
    ///
    /// This will be used to take from based on the indices returned by the lookup table to build the final output
    then_and_else_values: ArrayRef,
}

impl LiteralLookupTable {
    pub(in super::super) fn maybe_new(body: &CaseBody) -> Option<Self> {
        // We can't use the optimization if we don't have any when then pairs
        if body.when_then_expr.is_empty() {
            return None;
        }

        // If we only have 1 than this optimization is not useful
        if body.when_then_expr.len() == 1 {
            return None;
        }

        // Try to downcast all the WHEN/THEN expressions to literals
        let when_then_exprs_maybe_literals = body
            .when_then_expr
            .iter()
            .map(|(when, then)| {
                let when_maybe_literal = when.as_any().downcast_ref::<Literal>();
                let then_maybe_literal = then.as_any().downcast_ref::<Literal>();

                when_maybe_literal.zip(then_maybe_literal)
            })
            .collect::<Vec<_>>();

        // If not all the WHEN/THEN expressions are literals we cannot use this optimization
        if when_then_exprs_maybe_literals.contains(&None) {
            return None;
        }

        let when_then_exprs_scalars = when_then_exprs_maybe_literals
            .into_iter()
            // Unwrap the options as we have already checked there is no None
            .flatten()
            .map(|(when_lit, then_lit)| {
                (when_lit.value().clone(), then_lit.value().clone())
            })
            // Only keep non-null WHEN literals
            // as they cannot be matched - case NULL WHEN NULL THEN ... ELSE ... END always goes to ELSE
            .filter(|(when_lit, _)| !when_lit.is_null())
            .collect::<Vec<_>>();

        if when_then_exprs_scalars.is_empty() {
            // All WHEN literals were nulls, so cannot use optimization
            //
            // instead, another optimization would be to go straight to the ELSE clause
            return None;
        }

        // Keep only the first occurrence of each when literal (as the first match is used)
        // and remove nulls (as they cannot be matched - case NULL WHEN NULL THEN ... ELSE ... END always goes to ELSE)
        let (when, then): (Vec<ScalarValue>, Vec<ScalarValue>) = {
            let mut map = IndexMap::with_capacity(body.when_then_expr.len());

            for (when, then) in when_then_exprs_scalars.into_iter() {
                // Don't overwrite existing entries as we want to keep the first occurrence
                if !map.contains_key(&when) {
                    map.insert(when, then);
                }
            }

            map.into_iter().unzip()
        };

        let else_value: ScalarValue = if let Some(else_expr) = &body.else_expr {
            let literal = else_expr.as_any().downcast_ref::<Literal>()?;

            literal.value().clone()
        } else {
            let Ok(null_scalar) = ScalarValue::try_new_null(&then[0].data_type()) else {
                return None;
            };

            null_scalar
        };

        {
            let when_data_type = when[0].data_type();

            // If not all the WHEN literals are the same data type we cannot use this optimization
            if when.iter().any(|l| l.data_type() != when_data_type) {
                return None;
            }
        }

        {
            let data_type = then[0].data_type();

            // If not all the then and the else literals are the same data type we cannot use this optimization
            if then.iter().any(|l| l.data_type() != data_type) {
                return None;
            }

            if else_value.data_type() != data_type {
                return None;
            }
        }

        let then_and_else_values = ScalarValue::iter_to_array(
            then.iter()
                // The else is in the end
                .chain(std::iter::once(&else_value))
                .cloned(),
        )
        .ok()?;
        // The else expression is in the end
        let else_index = then_and_else_values.len() as u32 - 1;

        let lookup = try_creating_lookup_table(when).ok()?;

        Some(Self {
            lookup,
            then_and_else_values,
            else_index,
        })
    }

    pub(in super::super) fn map_keys_to_values(
        &self,
        keys_array: &ArrayRef,
    ) -> datafusion_common::Result<ArrayRef> {
        let take_indices = self
            .lookup
            .map_to_when_indices(keys_array, self.else_index)?;

        // Zero-copy conversion
        let take_indices = UInt32Array::from(take_indices);

        // An optimize version would depend on the type of the values_to_take_from
        // For example, if the type is view we can just keep pointing to the same value (similar to dictionary)
        // if the type is dictionary we can just use the indices as is (or cast them to the key type) and create a new dictionary array
        let output =
            arrow::compute::take(&self.then_and_else_values, &take_indices, None)
                .map_err(|e| arrow_datafusion_err!(e))?;

        Ok(output)
    }
}

/// Map values that match the WHEN literal to the index of their corresponding WHEN clause
///
/// For example, for this CASE expression:
///
/// ```sql
/// CASE <expr_a>
///     WHEN <literal_a> THEN <result_e>
///     WHEN <literal_b> THEN <result_f>
///     WHEN <literal_c> THEN <result_g>
///     WHEN <literal_d> THEN <result_h>
///     ELSE <fallback_result>
/// END
/// ```
///
/// this will map <literal_a> to 0, <literal_b> to 1, <literal_c> to 2, <literal_d> to 3
pub(super) trait WhenLiteralIndexMap: Debug + Send + Sync {
    /// Given an array of values, returns a vector of WHEN clause indices corresponding to each value in the provided array.
    ///
    /// For example, for this CASE expression:
    ///
    /// ```sql
    /// CASE <expr_a>
    ///     WHEN <literal_a> THEN <result_e>
    ///     WHEN <literal_b> THEN <result_f>
    ///     WHEN <literal_c> THEN <result_g>
    ///     WHEN <literal_d> THEN <result_h>
    ///     ELSE <fallback_result>
    /// END
    /// ```
    ///
    /// the array will be the evaluated values of `<expr_a>`
    /// and if that array is:
    /// - `[<literal_a>, <literal_c>, <literal_x>, <literal_b>, <literal_a>]`
    ///
    /// the returned vector will be:
    /// - `[0, 2, else_index, 1, 0]`
    ///
    fn map_to_when_indices(
        &self,
        array: &ArrayRef,
        else_index: u32,
    ) -> datafusion_common::Result<Vec<u32>>;
}

fn try_creating_lookup_table(
    unique_non_null_literals: Vec<ScalarValue>,
) -> datafusion_common::Result<Box<dyn WhenLiteralIndexMap>> {
    assert_ne!(
        unique_non_null_literals.len(),
        0,
        "Must have at least one literal"
    );
    match unique_non_null_literals[0].data_type() {
        DataType::Boolean => {
            let lookup_table = BooleanIndexMap::try_new(unique_non_null_literals)?;
            Ok(Box::new(lookup_table))
        }

        data_type if data_type.is_primitive() => {
            macro_rules! create_matching_map {
                ($t:ty) => {{
                    let lookup_table =
                        PrimitiveIndexMap::<$t>::try_new(unique_non_null_literals)?;
                    Ok(Box::new(lookup_table))
                }};
            }

            downcast_primitive! {
                data_type => (create_matching_map),
                _ => Err(plan_datafusion_err!(
                    "Unsupported field type for primitive: {:?}",
                    data_type
                )),
            }
        }

        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Utf8View
        | DataType::BinaryView => {
            let lookup_table = BytesLikeIndexMap::try_new(unique_non_null_literals)?;
            Ok(Box::new(lookup_table))
        }

        DataType::Dictionary(_key, value)
            if matches!(
                value.as_ref(),
                DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::FixedSizeBinary(_)
                    | DataType::Utf8View
                    | DataType::BinaryView
            ) =>
        {
            let lookup_table = BytesLikeIndexMap::try_new(unique_non_null_literals)?;
            Ok(Box::new(lookup_table))
        }

        _ => Err(plan_datafusion_err!(
            "Unsupported data type for lookup table: {}",
            unique_non_null_literals[0].data_type()
        )),
    }
}
