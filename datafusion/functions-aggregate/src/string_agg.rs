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

//! [`StringAgg`] accumulator for the `string_agg` function

use std::any::Any;
use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

use crate::array_agg::ArrayAgg;

use arrow::array::{ArrayRef, AsArray, BooleanArray, LargeStringArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{
    Result, ScalarValue, internal_datafusion_err, internal_err, not_impl_err,
};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions_aggregate_common::accumulator::StateFieldsArgs;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::apply_filter_as_nulls;
use datafusion_macros::user_doc;
use datafusion_physical_expr::expressions::Literal;

make_udaf_expr_and_func!(
    StringAgg,
    string_agg,
    expr delimiter,
    "Concatenates the values of string expressions and places separator values between them",
    string_agg_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Concatenates the values of string expressions and places separator values between them. \
If ordering is required, strings are concatenated in the specified order. \
This aggregation function can only mix DISTINCT and ORDER BY if the ordering expression is exactly the same as the first argument expression.",
    syntax_example = "string_agg([DISTINCT] expression, delimiter [ORDER BY expression])",
    sql_example = r#"```sql
> SELECT string_agg(name, ', ') AS names_list
  FROM employee;
+--------------------------+
| names_list               |
+--------------------------+
| Alice, Bob, Bob, Charlie |
+--------------------------+
> SELECT string_agg(name, ', ' ORDER BY name DESC) AS names_list
  FROM employee;
+--------------------------+
| names_list               |
+--------------------------+
| Charlie, Bob, Bob, Alice |
+--------------------------+
> SELECT string_agg(DISTINCT name, ', ' ORDER BY name DESC) AS names_list
  FROM employee;
+--------------------------+
| names_list               |
+--------------------------+
| Charlie, Bob, Alice |
+--------------------------+
```"#,
    argument(
        name = "expression",
        description = "The string expression to concatenate. Can be a column or any valid string expression."
    ),
    argument(
        name = "delimiter",
        description = "A literal string used as a separator between the concatenated values."
    )
)]
/// STRING_AGG aggregate expression
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct StringAgg {
    signature: Signature,
    array_agg: ArrayAgg,
}

impl StringAgg {
    /// Create a new StringAgg aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Null]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Null]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Null]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8]),
                ],
                Volatility::Immutable,
            ),
            array_agg: Default::default(),
        }
    }

    /// Extract the delimiter string from the second argument expression.
    fn extract_delimiter(args: &AccumulatorArgs) -> Result<String> {
        let Some(lit) = args.exprs[1].as_any().downcast_ref::<Literal>() else {
            return not_impl_err!("string_agg delimiter must be a string literal");
        };

        if lit.value().is_null() {
            return Ok(String::new());
        }

        match lit.value().try_as_str() {
            Some(s) => Ok(s.unwrap_or("").to_string()),
            None => {
                not_impl_err!(
                    "string_agg not supported for delimiter \"{}\"",
                    lit.value()
                )
            }
        }
    }
}

impl Default for StringAgg {
    fn default() -> Self {
        Self::new()
    }
}

/// Three accumulation strategies depending on query shape:
/// - No DISTINCT / ORDER BY with GROUP BY: `StringAggGroupsAccumulator`
/// - No DISTINCT / ORDER BY without GROUP BY: `SimpleStringAggAccumulator`
/// - With DISTINCT or ORDER BY: `StringAggAccumulator` (delegates to `ArrayAgg`)
impl AggregateUDFImpl for StringAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "string_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::LargeUtf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if !args.is_distinct && args.ordering_fields.is_empty() {
            Ok(vec![
                Field::new(
                    format_state_name(args.name, "string_agg"),
                    DataType::LargeUtf8,
                    true,
                )
                .into(),
            ])
        } else {
            self.array_agg.state_fields(args)
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let delimiter = Self::extract_delimiter(&acc_args)?;

        if !acc_args.is_distinct && acc_args.order_bys.is_empty() {
            Ok(Box::new(SimpleStringAggAccumulator::new(&delimiter)))
        } else {
            let array_agg_acc = self.array_agg.accumulator(AccumulatorArgs {
                return_field: Field::new(
                    "f",
                    DataType::new_list(acc_args.return_field.data_type().clone(), true),
                    true,
                )
                .into(),
                exprs: &filter_index(acc_args.exprs, 1),
                expr_fields: &filter_index(acc_args.expr_fields, 1),
                // Unchanged below; we list each field explicitly in case we ever add more
                // fields to AccumulatorArgs making it easier to see if changes are also
                // needed here.
                schema: acc_args.schema,
                ignore_nulls: acc_args.ignore_nulls,
                order_bys: acc_args.order_bys,
                is_reversed: acc_args.is_reversed,
                name: acc_args.name,
                is_distinct: acc_args.is_distinct,
            })?;

            Ok(Box::new(StringAggAccumulator::new(
                array_agg_acc,
                &delimiter,
            )))
        }
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Reversed(string_agg_udaf())
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct && args.order_bys.is_empty()
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let delimiter = Self::extract_delimiter(&args)?;
        Ok(Box::new(StringAggGroupsAccumulator::new(delimiter)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// StringAgg accumulator for the general case (with order or distinct specified)
#[derive(Debug)]
pub(crate) struct StringAggAccumulator {
    array_agg_acc: Box<dyn Accumulator>,
    delimiter: String,
}

impl StringAggAccumulator {
    pub fn new(array_agg_acc: Box<dyn Accumulator>, delimiter: &str) -> Self {
        Self {
            array_agg_acc,
            delimiter: delimiter.to_string(),
        }
    }
}

impl Accumulator for StringAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.array_agg_acc.update_batch(&filter_index(values, 1))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let scalar = self.array_agg_acc.evaluate()?;

        let ScalarValue::List(list) = scalar else {
            return internal_err!(
                "Expected a DataType::List while evaluating underlying ArrayAggAccumulator, but got {}",
                scalar.data_type()
            );
        };

        let string_arr: Vec<_> = match list.value_type() {
            DataType::LargeUtf8 => as_generic_string_array::<i64>(list.values())?
                .iter()
                .flatten()
                .collect(),
            DataType::Utf8 => as_generic_string_array::<i32>(list.values())?
                .iter()
                .flatten()
                .collect(),
            DataType::Utf8View => as_string_view_array(list.values())?
                .iter()
                .flatten()
                .collect(),
            _ => {
                return internal_err!(
                    "Expected elements to of type Utf8 or LargeUtf8, but got {}",
                    list.value_type()
                );
            }
        };

        if string_arr.is_empty() {
            return Ok(ScalarValue::LargeUtf8(None));
        }

        Ok(ScalarValue::LargeUtf8(Some(
            string_arr.join(&self.delimiter),
        )))
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.array_agg_acc)
            + self.array_agg_acc.size()
            + self.delimiter.capacity()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.array_agg_acc.state()
    }

    fn merge_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.array_agg_acc.merge_batch(values)
    }
}

fn filter_index<T: Clone>(values: &[T], index: usize) -> Vec<T> {
    values
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != index)
        .map(|(_, v)| v)
        .cloned()
        .collect::<Vec<_>>()
}

/// GroupsAccumulator for `string_agg` without DISTINCT or ORDER BY.
#[derive(Debug)]
struct StringAggGroupsAccumulator {
    /// The delimiter placed between concatenated values.
    delimiter: String,
    /// Accumulated string per group. `None` means no values have been seen
    /// (the group's output will be NULL).
    /// A potential improvement is to avoid this String allocation
    /// See <https://github.com/apache/datafusion/issues/21156>
    values: Vec<Option<String>>,
    /// Running total of string data bytes across all groups.
    total_data_bytes: usize,
}

impl StringAggGroupsAccumulator {
    fn new(delimiter: String) -> Self {
        Self {
            delimiter,
            values: Vec::new(),
            total_data_bytes: 0,
        }
    }

    fn append_batch<'a>(
        &mut self,
        iter: impl Iterator<Item = Option<&'a str>>,
        group_indices: &[usize],
    ) {
        for (opt_value, &group_idx) in iter.zip(group_indices.iter()) {
            if let Some(value) = opt_value {
                match &mut self.values[group_idx] {
                    Some(existing) => {
                        let added = self.delimiter.len() + value.len();
                        existing.reserve(added);
                        existing.push_str(&self.delimiter);
                        existing.push_str(value);
                        self.total_data_bytes += added;
                    }
                    slot @ None => {
                        *slot = Some(value.to_string());
                        self.total_data_bytes += value.len();
                    }
                }
            }
        }
    }
}

impl GroupsAccumulator for StringAggGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.values.resize(total_num_groups, None);
        let array = apply_filter_as_nulls(&values[0], opt_filter)?;
        match array.data_type() {
            DataType::Utf8 => {
                self.append_batch(array.as_string::<i32>().iter(), group_indices)
            }
            DataType::LargeUtf8 => {
                self.append_batch(array.as_string::<i64>().iter(), group_indices)
            }
            DataType::Utf8View => {
                self.append_batch(array.as_string_view().iter(), group_indices)
            }
            other => {
                return internal_err!("string_agg unexpected data type: {other}");
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let to_emit = emit_to.take_needed(&mut self.values);
        let emitted_bytes: usize = to_emit
            .iter()
            .filter_map(|opt| opt.as_ref().map(|s| s.len()))
            .sum();
        self.total_data_bytes -= emitted_bytes;

        let result: ArrayRef = Arc::new(LargeStringArray::from(to_emit));
        Ok(result)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // State is always LargeUtf8, which update_batch already handles.
        self.update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let input = apply_filter_as_nulls(&values[0], opt_filter)?;
        let result = if input.data_type() == &DataType::LargeUtf8 {
            input
        } else {
            arrow::compute::cast(&input, &DataType::LargeUtf8)?
        };
        Ok(vec![result])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.total_data_bytes
            + self.values.capacity() * size_of::<Option<String>>()
            + self.delimiter.capacity()
            + size_of_val(self)
    }
}

/// Per-row accumulator for `string_agg` without DISTINCT or ORDER BY.  Used for
/// non-grouped aggregation; grouped queries use [`StringAggGroupsAccumulator`].
#[derive(Debug)]
pub(crate) struct SimpleStringAggAccumulator {
    delimiter: String,
    /// Updated during `update_batch()`. e.g. "foo,bar"
    accumulated_string: String,
    has_value: bool,
}

impl SimpleStringAggAccumulator {
    pub fn new(delimiter: &str) -> Self {
        Self {
            delimiter: delimiter.to_string(),
            accumulated_string: String::new(),
            has_value: false,
        }
    }

    #[inline]
    fn append_strings<'a, I>(&mut self, iter: I)
    where
        I: Iterator<Item = Option<&'a str>>,
    {
        for value in iter.flatten() {
            if self.has_value {
                self.accumulated_string.push_str(&self.delimiter);
            }

            self.accumulated_string.push_str(value);
            self.has_value = true;
        }
    }
}

impl Accumulator for SimpleStringAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let string_arr = values.first().ok_or_else(|| {
            internal_datafusion_err!(
                "Planner should ensure its first arg is Utf8/Utf8View"
            )
        })?;

        match string_arr.data_type() {
            DataType::Utf8 => self.append_strings(string_arr.as_string::<i32>().iter()),
            DataType::LargeUtf8 => {
                self.append_strings(string_arr.as_string::<i64>().iter())
            }
            DataType::Utf8View => self.append_strings(string_arr.as_string_view().iter()),
            other => {
                return internal_err!(
                    "Planner should ensure string_agg first argument is Utf8-like, found {other}"
                );
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.has_value {
            Ok(ScalarValue::LargeUtf8(Some(
                self.accumulated_string.clone(),
            )))
        } else {
            Ok(ScalarValue::LargeUtf8(None))
        }
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.delimiter.capacity() + self.accumulated_string.capacity()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let result = if self.has_value {
            ScalarValue::LargeUtf8(Some(std::mem::take(&mut self.accumulated_string)))
        } else {
            ScalarValue::LargeUtf8(None)
        };
        self.has_value = false;

        Ok(vec![result])
    }

    fn merge_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.update_batch(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::LargeStringArray;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{Fields, Schema};
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
    use std::sync::Arc;

    #[test]
    fn no_duplicates_no_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",").build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"]), data([","])])?;
        acc2.update_batch(&[data(["d", "e", "f"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str(acc1.evaluate()?);

        assert_eq!(result, "a,b,c,d,e,f");

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",")
            .distinct()
            .build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"]), data([","])])?;
        acc2.update_batch(&[data(["d", "e", "f"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str_sorted(acc1.evaluate()?, ",");

        assert_eq!(result, "a,b,c,d,e,f");

        Ok(())
    }

    #[test]
    fn duplicates_no_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",").build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"]), data([","])])?;
        acc2.update_batch(&[data(["a", "b", "c"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str(acc1.evaluate()?);

        assert_eq!(result, "a,b,c,a,b,c");

        Ok(())
    }

    #[test]
    fn duplicates_distinct() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",")
            .distinct()
            .build_two()?;

        acc1.update_batch(&[data(["a", "b", "c"]), data([","])])?;
        acc2.update_batch(&[data(["a", "b", "c"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str_sorted(acc1.evaluate()?, ",");

        assert_eq!(result, "a,b,c");

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_asc() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",")
            .distinct()
            .order_by_col("col", SortOptions::new(false, false))
            .build_two()?;

        acc1.update_batch(&[data(["e", "b", "d"]), data([","])])?;
        acc2.update_batch(&[data(["f", "a", "c"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str(acc1.evaluate()?);

        assert_eq!(result, "a,b,c,d,e,f");

        Ok(())
    }

    #[test]
    fn no_duplicates_distinct_sort_desc() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",")
            .distinct()
            .order_by_col("col", SortOptions::new(true, false))
            .build_two()?;

        acc1.update_batch(&[data(["e", "b", "d"]), data([","])])?;
        acc2.update_batch(&[data(["f", "a", "c"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str(acc1.evaluate()?);

        assert_eq!(result, "f,e,d,c,b,a");

        Ok(())
    }

    #[test]
    fn duplicates_distinct_sort_asc() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",")
            .distinct()
            .order_by_col("col", SortOptions::new(false, false))
            .build_two()?;

        acc1.update_batch(&[data(["a", "c", "b"]), data([","])])?;
        acc2.update_batch(&[data(["b", "c", "a"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str(acc1.evaluate()?);

        assert_eq!(result, "a,b,c");

        Ok(())
    }

    #[test]
    fn duplicates_distinct_sort_desc() -> Result<()> {
        let (mut acc1, mut acc2) = StringAggAccumulatorBuilder::new(",")
            .distinct()
            .order_by_col("col", SortOptions::new(true, false))
            .build_two()?;

        acc1.update_batch(&[data(["a", "c", "b"]), data([","])])?;
        acc2.update_batch(&[data(["b", "c", "a"]), data([","])])?;
        acc1 = merge(acc1, acc2)?;

        let result = some_str(acc1.evaluate()?);

        assert_eq!(result, "c,b,a");

        Ok(())
    }

    struct StringAggAccumulatorBuilder {
        sep: String,
        distinct: bool,
        order_bys: Vec<PhysicalSortExpr>,
        schema: Schema,
    }

    impl StringAggAccumulatorBuilder {
        fn new(sep: &str) -> Self {
            Self {
                sep: sep.to_string(),
                distinct: Default::default(),
                order_bys: vec![],
                schema: Schema {
                    fields: Fields::from(vec![Field::new(
                        "col",
                        DataType::LargeUtf8,
                        true,
                    )]),
                    metadata: Default::default(),
                },
            }
        }
        fn distinct(mut self) -> Self {
            self.distinct = true;
            self
        }

        fn order_by_col(mut self, col: &str, sort_options: SortOptions) -> Self {
            self.order_bys.extend([PhysicalSortExpr::new(
                Arc::new(
                    Column::new_with_schema(col, &self.schema)
                        .expect("column not available in schema"),
                ),
                sort_options,
            )]);
            self
        }

        fn build(&self) -> Result<Box<dyn Accumulator>> {
            StringAgg::new().accumulator(AccumulatorArgs {
                return_field: Field::new("f", DataType::LargeUtf8, true).into(),
                schema: &self.schema,
                expr_fields: &[
                    Field::new("col", DataType::LargeUtf8, true).into(),
                    Field::new("lit", DataType::Utf8, false).into(),
                ],
                ignore_nulls: false,
                order_bys: &self.order_bys,
                is_reversed: false,
                name: "",
                is_distinct: self.distinct,
                exprs: &[
                    Arc::new(Column::new("col", 0)),
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(self.sep.to_string())))),
                ],
            })
        }

        fn build_two(&self) -> Result<(Box<dyn Accumulator>, Box<dyn Accumulator>)> {
            Ok((self.build()?, self.build()?))
        }
    }

    fn some_str(value: ScalarValue) -> String {
        str(value)
            .expect("ScalarValue was not a String")
            .expect("ScalarValue was None")
    }

    fn some_str_sorted(value: ScalarValue, sep: &str) -> String {
        let value = some_str(value);
        let mut parts: Vec<&str> = value.split(sep).collect();
        parts.sort();
        parts.join(sep)
    }

    fn str(value: ScalarValue) -> Result<Option<String>> {
        match value {
            ScalarValue::LargeUtf8(v) => Ok(v),
            _ => internal_err!(
                "Expected ScalarValue::LargeUtf8, got {}",
                value.data_type()
            ),
        }
    }

    fn data<const N: usize>(list: [&str; N]) -> ArrayRef {
        Arc::new(LargeStringArray::from(list.to_vec()))
    }

    fn merge(
        mut acc1: Box<dyn Accumulator>,
        mut acc2: Box<dyn Accumulator>,
    ) -> Result<Box<dyn Accumulator>> {
        let intermediate_state = acc2.state().and_then(|e| {
            e.iter()
                .map(|v| v.to_array())
                .collect::<Result<Vec<ArrayRef>>>()
        })?;
        acc1.merge_batch(&intermediate_state)?;
        Ok(acc1)
    }

    // ---------------------------------------------------------------
    // Tests for StringAggGroupsAccumulator
    // ---------------------------------------------------------------

    fn make_groups_acc(delimiter: &str) -> StringAggGroupsAccumulator {
        StringAggGroupsAccumulator::new(delimiter.to_string())
    }

    /// Helper: evaluate and downcast to LargeStringArray
    fn evaluate_groups(
        acc: &mut StringAggGroupsAccumulator,
        emit_to: EmitTo,
    ) -> Vec<Option<String>> {
        let result = acc.evaluate(emit_to).unwrap();
        let arr = result.as_any().downcast_ref::<LargeStringArray>().unwrap();
        arr.iter().map(|v| v.map(|s| s.to_string())).collect()
    }

    #[test]
    fn groups_basic() -> Result<()> {
        let mut acc = make_groups_acc(",");

        // 6 rows, 3 groups: group 0 gets "a","d"; group 1 gets "b","e"; group 2 gets "c","f"
        let values: ArrayRef =
            Arc::new(LargeStringArray::from(vec!["a", "b", "c", "d", "e", "f"]));
        let group_indices = vec![0, 1, 2, 0, 1, 2];
        acc.update_batch(&[values], &group_indices, None, 3)?;

        let result = evaluate_groups(&mut acc, EmitTo::All);
        assert_eq!(
            result,
            vec![
                Some("a,d".to_string()),
                Some("b,e".to_string()),
                Some("c,f".to_string()),
            ]
        );
        Ok(())
    }

    #[test]
    fn groups_with_nulls() -> Result<()> {
        let mut acc = make_groups_acc("|");

        // Group 0: "a", NULL, "c" → "a|c"
        // Group 1: NULL, "b"     → "b"
        // Group 2: NULL only     → NULL
        let values: ArrayRef = Arc::new(LargeStringArray::from(vec![
            Some("a"),
            None,
            Some("c"),
            None,
            Some("b"),
            None,
        ]));
        let group_indices = vec![0, 1, 0, 2, 1, 2];
        acc.update_batch(&[values], &group_indices, None, 3)?;

        let result = evaluate_groups(&mut acc, EmitTo::All);
        assert_eq!(
            result,
            vec![Some("a|c".to_string()), Some("b".to_string()), None,]
        );
        Ok(())
    }

    #[test]
    fn groups_with_filter() -> Result<()> {
        let mut acc = make_groups_acc(",");

        let values: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "b", "c", "d"]));
        let group_indices = vec![0, 0, 1, 1];
        // Filter: only rows 0 and 3 are included
        let filter = BooleanArray::from(vec![true, false, false, true]);
        acc.update_batch(&[values], &group_indices, Some(&filter), 2)?;

        let result = evaluate_groups(&mut acc, EmitTo::All);
        assert_eq!(result, vec![Some("a".to_string()), Some("d".to_string())]);
        Ok(())
    }

    #[test]
    fn groups_emit_first() -> Result<()> {
        let mut acc = make_groups_acc(",");

        let values: ArrayRef =
            Arc::new(LargeStringArray::from(vec!["a", "b", "c", "d", "e", "f"]));
        let group_indices = vec![0, 1, 2, 0, 1, 2];
        acc.update_batch(&[values], &group_indices, None, 3)?;

        // Emit only the first 2 groups
        let result = evaluate_groups(&mut acc, EmitTo::First(2));
        assert_eq!(
            result,
            vec![Some("a,d".to_string()), Some("b,e".to_string())]
        );

        // Group 2 (now shifted to index 0) should still be intact
        let result = evaluate_groups(&mut acc, EmitTo::All);
        assert_eq!(result, vec![Some("c,f".to_string())]);
        Ok(())
    }

    #[test]
    fn groups_merge_batch() -> Result<()> {
        let mut acc = make_groups_acc(",");

        // First batch: group 0 = "a", group 1 = "b"
        let values: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "b"]));
        acc.update_batch(&[values], &[0, 1], None, 2)?;

        // Simulate a second accumulator's state (LargeUtf8 partial strings)
        let partial_state: ArrayRef = Arc::new(LargeStringArray::from(vec!["c,d", "e"]));
        acc.merge_batch(&[partial_state], &[0, 1], None, 2)?;

        let result = evaluate_groups(&mut acc, EmitTo::All);
        assert_eq!(
            result,
            vec![Some("a,c,d".to_string()), Some("b,e".to_string())]
        );
        Ok(())
    }

    #[test]
    fn groups_empty_groups() -> Result<()> {
        let mut acc = make_groups_acc(",");

        // 4 groups total, but only groups 0 and 2 receive values
        let values: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "b"]));
        acc.update_batch(&[values], &[0, 2], None, 4)?;

        let result = evaluate_groups(&mut acc, EmitTo::All);
        assert_eq!(
            result,
            vec![
                Some("a".to_string()),
                None, // group 1: never received a value
                Some("b".to_string()),
                None, // group 3: never received a value
            ]
        );
        Ok(())
    }

    #[test]
    fn groups_multiple_batches() -> Result<()> {
        let mut acc = make_groups_acc("|");

        // Batch 1: 2 groups
        let values: ArrayRef = Arc::new(LargeStringArray::from(vec!["a", "b"]));
        acc.update_batch(&[values], &[0, 1], None, 2)?;

        // Batch 2: same groups, plus a new group
        let values: ArrayRef = Arc::new(LargeStringArray::from(vec!["c", "d", "e"]));
        acc.update_batch(&[values], &[0, 1, 2], None, 3)?;

        let result = evaluate_groups(&mut acc, EmitTo::All);
        assert_eq!(
            result,
            vec![
                Some("a|c".to_string()),
                Some("b|d".to_string()),
                Some("e".to_string()),
            ]
        );
        Ok(())
    }
}
