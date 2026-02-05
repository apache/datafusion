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

use arrow::array::{
    Array, BooleanArray, Capacities, MutableArrayData, Scalar, make_array,
    make_comparator,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow_buffer::NullBuffer;

use datafusion_common::cast::{as_map_array, as_struct_array};
use datafusion_common::{
    Result, ScalarValue, exec_err, internal_err, plan_datafusion_err,
};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::ExprSimplifyResult;
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = r#"Returns a field within a map or a struct with the given key.
    Supports nested field access by providing multiple field names.
    Note: most users invoke `get_field` indirectly via field access
    syntax such as `my_struct_col['field_name']` which results in a call to
    `get_field(my_struct_col, 'field_name')`.
    Nested access like `my_struct['a']['b']` is optimized to a single call:
    `get_field(my_struct, 'a', 'b')`."#,
    syntax_example = "get_field(expression, field_name[, field_name2, ...])",
    sql_example = r#"```sql
> -- Access a field from a struct column
> create table test( struct_col) as values
    ({name: 'Alice', age: 30}),
    ({name: 'Bob', age: 25});
> select struct_col from test;
+-----------------------------+
| struct_col                  |
+-----------------------------+
| {name: Alice, age: 30}      |
| {name: Bob, age: 25}        |
+-----------------------------+
> select struct_col['name'] as name from test;
+-------+
| name  |
+-------+
| Alice |
| Bob   |
+-------+

> -- Nested field access with multiple arguments
> create table test(struct_col) as values
    ({outer: {inner_val: 42}});
> select struct_col['outer']['inner_val'] as result from test;
+--------+
| result |
+--------+
| 42     |
+--------+
```"#,
    argument(
        name = "expression",
        description = "The map or struct to retrieve a field from."
    ),
    argument(
        name = "field_name",
        description = "The field name(s) to access, in order for nested access. Must evaluate to strings."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GetFieldFunc {
    signature: Signature,
}

impl Default for GetFieldFunc {
    fn default() -> Self {
        Self::new()
    }
}

/// Process a map array by finding matching keys and extracting corresponding values.
///
/// This function handles both simple (scalar) and nested key types by using
/// appropriate comparison strategies.
fn process_map_array(
    array: &dyn Array,
    key_array: Arc<dyn Array>,
) -> Result<ColumnarValue> {
    let map_array = as_map_array(array)?;
    let keys = if key_array.data_type().is_nested() {
        let comparator = make_comparator(
            map_array.keys().as_ref(),
            key_array.as_ref(),
            SortOptions::default(),
        )?;
        let len = map_array.keys().len().min(key_array.len());
        let values = (0..len).map(|i| comparator(i, i).is_eq()).collect();
        let nulls = NullBuffer::union(map_array.keys().nulls(), key_array.nulls());
        BooleanArray::new(values, nulls)
    } else {
        let be_compared = Scalar::new(key_array);
        arrow::compute::kernels::cmp::eq(&be_compared, map_array.keys())?
    };

    let original_data = map_array.entries().column(1).to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for entry in 0..map_array.len() {
        let start = map_array.value_offsets()[entry] as usize;
        let end = map_array.value_offsets()[entry + 1] as usize;

        let maybe_matched = keys
            .slice(start, end - start)
            .iter()
            .enumerate()
            .find(|(_, t)| t.unwrap());

        if maybe_matched.is_none() {
            mutable.extend_nulls(1);
            continue;
        }
        let (match_offset, _) = maybe_matched.unwrap();
        mutable.extend(0, start + match_offset, start + match_offset + 1);
    }

    let data = mutable.freeze();
    let data = make_array(data);
    Ok(ColumnarValue::Array(data))
}

/// Process a map array with a nested key type by iterating through entries
/// and using a comparator for key matching.
///
/// This specialized version is used when the key type is nested (e.g., struct, list).
fn process_map_with_nested_key(
    array: &dyn Array,
    key_array: &dyn Array,
) -> Result<ColumnarValue> {
    let map_array = as_map_array(array)?;

    let comparator =
        make_comparator(map_array.keys().as_ref(), key_array, SortOptions::default())?;

    let original_data = map_array.entries().column(1).to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    for entry in 0..map_array.len() {
        let start = map_array.value_offsets()[entry] as usize;
        let end = map_array.value_offsets()[entry + 1] as usize;

        let mut found_match = false;
        for i in start..end {
            if comparator(i, 0).is_eq() {
                mutable.extend(0, i, i + 1);
                found_match = true;
                break;
            }
        }

        if !found_match {
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    let data = make_array(data);
    Ok(ColumnarValue::Array(data))
}

/// Extract a single field from a struct or map array
fn extract_single_field(base: ColumnarValue, name: ScalarValue) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&[base])?;
    let array = Arc::clone(&arrays[0]);

    let string_value = name.try_as_str().flatten().map(|s| s.to_string());

    match (array.data_type(), name, string_value) {
        (DataType::Map(_, _), ScalarValue::List(arr), _) => {
            let key_array: Arc<dyn Array> = arr;
            process_map_array(&array, key_array)
        }
        (DataType::Map(_, _), ScalarValue::Struct(arr), _) => {
            process_map_array(&array, arr as Arc<dyn Array>)
        }
        (DataType::Map(_, _), other, _) => {
            let data_type = other.data_type();
            if data_type.is_nested() {
                process_map_with_nested_key(&array, &other.to_array()?)
            } else {
                process_map_array(&array, other.to_array()?)
            }
        }
        (DataType::Struct(_), _, Some(k)) => {
            let as_struct_array = as_struct_array(&array)?;
            match as_struct_array.column_by_name(&k) {
                None => exec_err!("Field {k} not found in struct"),
                Some(col) => Ok(ColumnarValue::Array(Arc::clone(col))),
            }
        }
        (DataType::Struct(_), name, _) => exec_err!(
            "get_field is only possible on struct with utf8 indexes. \
                         Received with {name:?} index"
        ),
        (DataType::Null, _, _) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        (dt, name, _) => exec_err!(
            "get_field is only possible on maps or structs. Received {dt} with {name:?} index"
        ),
    }
}

impl GetFieldFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

// get_field(struct_array, field_name)
impl ScalarUDFImpl for GetFieldFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "get_field"
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.len()
            );
        }

        let base = &args[0];
        let field_names: Vec<String> = args[1..]
            .iter()
            .map(|f| match f {
                Expr::Literal(name, _) => name.to_string(),
                other => other.schema_name().to_string(),
            })
            .collect();

        Ok(format!("{}[{}]", base, field_names.join("][")))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.len()
            );
        }

        let base = &args[0];
        let field_names: Vec<String> = args[1..]
            .iter()
            .map(|f| match f {
                Expr::Literal(name, _) => name.to_string(),
                other => other.schema_name().to_string(),
            })
            .collect();

        Ok(format!(
            "{}[{}]",
            base.schema_name(),
            field_names.join("][")
        ))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Validate minimum 2 arguments: base expression + at least one field name
        if args.scalar_arguments.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.scalar_arguments.len()
            );
        }

        let mut current_field = Arc::clone(&args.arg_fields[0]);

        // Iterate through each field name (starting from index 1)
        for (i, sv) in args.scalar_arguments.iter().enumerate().skip(1) {
            match current_field.data_type() {
                DataType::Map(map_field, _) => {
                    match map_field.data_type() {
                        DataType::Struct(fields) if fields.len() == 2 => {
                            // Arrow's MapArray is essentially a ListArray of structs with two columns. They are
                            // often named "key", and "value", but we don't require any specific naming here;
                            // instead, we assume that the second column is the "value" column both here and in
                            // execution.
                            let value_field = fields
                                .get(1)
                                .expect("fields should have exactly two members");

                            current_field = Arc::new(
                                value_field.as_ref().clone().with_nullable(true),
                            );
                        }
                        _ => {
                            return exec_err!(
                                "Map fields must contain a Struct with exactly 2 fields"
                            );
                        }
                    }
                }
                DataType::Struct(fields) => {
                    let field_name = sv
                        .as_ref()
                        .and_then(|sv| {
                            sv.try_as_str().flatten().filter(|s| !s.is_empty())
                        })
                        .ok_or_else(|| {
                            datafusion_common::DataFusionError::Execution(
                                "Field name must be a non-empty string".to_string(),
                            )
                        })?;

                    let child_field = fields
                        .iter()
                        .find(|f| f.name() == field_name)
                        .ok_or_else(|| {
                            plan_datafusion_err!("Field {field_name} not found in struct")
                        })?;

                    let mut new_field = child_field.as_ref().clone();

                    // If the parent is nullable, then getting the child must be nullable
                    if current_field.is_nullable() {
                        new_field = new_field.with_nullable(true);
                    }
                    current_field = Arc::new(new_field);
                }
                DataType::Null => {
                    return Ok(Field::new(self.name(), DataType::Null, true).into());
                }
                other => {
                    return exec_err!(
                        "Cannot access field at argument {}: type {} is not Struct, Map, or Null",
                        i,
                        other
                    );
                }
            }
        }

        Ok(current_field)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                args.args.len()
            );
        }

        let mut current = args.args[0].clone();

        // Early exit for null base
        if current.data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }

        // Iterate through each field name
        for field_name in args.args.iter().skip(1) {
            let field_name_scalar = match field_name {
                ColumnarValue::Scalar(name) => name.clone(),
                _ => {
                    return exec_err!(
                        "get_field function requires all field_name arguments to be scalars"
                    );
                }
            };

            current = extract_single_field(current, field_name_scalar)?;

            // Early exit if we hit null
            if current.data_type().is_null() {
                return Ok(ColumnarValue::Scalar(ScalarValue::Null));
            }
        }

        Ok(current)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &datafusion_expr::simplify::SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        // Need at least 2 args (base + field)
        if args.len() < 2 {
            return Ok(ExprSimplifyResult::Original(args));
        }

        // Flatten all nested get_field calls in a single pass
        // Pattern: get_field(get_field(get_field(base, a), b), c) => get_field(base, a, b, c)

        // Collect path arguments from all nested levels
        let mut path_args_stack = Vec::new();
        let mut current_expr = &args[0];

        // Push the outermost path arguments first
        path_args_stack.push(&args[1..]);

        // Walk down the chain of nested get_field calls
        let base_expr = loop {
            if let Expr::ScalarFunction(ScalarFunction {
                func,
                args: inner_args,
            }) = current_expr
                && func
                    .inner()
                    .as_any()
                    .downcast_ref::<GetFieldFunc>()
                    .is_some()
            {
                // Store this level's path arguments (all except the first, which is base/nested call)
                path_args_stack.push(&inner_args[1..]);

                // Move to the next level down
                current_expr = &inner_args[0];
                continue;
            }
            // Not a get_field call, this is the base expression
            break current_expr;
        };

        // If no nested get_field calls were found, return original
        if path_args_stack.len() == args.len() - 1 {
            return Ok(ExprSimplifyResult::Original(args));
        }

        // If we found any nested get_field calls, flatten them
        // Build merged args: [base, ...all_path_args_in_correct_order]
        let mut merged_args = vec![base_expr.clone()];

        // Add path args in reverse order (innermost to outermost)
        // Stack is: [outermost_paths, ..., innermost_paths]
        // We want: [base, innermost_paths, ..., outermost_paths]
        for path_slice in path_args_stack.iter().rev() {
            merged_args.extend_from_slice(path_slice);
        }

        Ok(ExprSimplifyResult::Simplified(Expr::ScalarFunction(
            ScalarFunction::new_udf(
                Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::new())),
                merged_args,
            ),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 {
            return exec_err!(
                "get_field requires at least 2 arguments, got {}",
                arg_types.len()
            );
        }
        // Accept types as-is, validation happens in return_field_from_args
        Ok(arg_types.to_vec())
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        // get_field can be pushed to leaves if:
        // 1. The base (first arg) is a column or already placeable at leaves
        // 2. All field keys (remaining args) are literals
        if args.is_empty() {
            return ExpressionPlacement::KeepInPlace;
        }

        let base_placement = args[0];
        let base_is_pushable = matches!(
            base_placement,
            ExpressionPlacement::Column | ExpressionPlacement::MoveTowardsLeafNodes
        );

        let all_keys_are_literals = args
            .iter()
            .skip(1)
            .all(|p| matches!(p, ExpressionPlacement::Literal));

        if base_is_pushable && all_keys_are_literals {
            ExpressionPlacement::MoveTowardsLeafNodes
        } else {
            ExpressionPlacement::KeepInPlace
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StructArray};
    use arrow::datatypes::Fields;

    #[test]
    fn test_get_field_utf8view_key() -> Result<()> {
        // Create a struct array with fields "a" and "b"
        let a_values = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let b_values = Int32Array::from(vec![Some(10), Some(20), Some(30)]);

        let fields: Fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]
        .into();

        let struct_array = StructArray::new(
            fields,
            vec![
                Arc::new(a_values) as ArrayRef,
                Arc::new(b_values) as ArrayRef,
            ],
            None,
        );

        let base = ColumnarValue::Array(Arc::new(struct_array));

        // Use Utf8View key to access field "a"
        let key = ScalarValue::Utf8View(Some("a".to_string()));

        let result = extract_single_field(base, key)?;

        let result_array = result.into_array(3)?;
        let expected = Int32Array::from(vec![Some(1), Some(2), Some(3)]);

        assert_eq!(result_array.as_ref(), &expected as &dyn Array);

        Ok(())
    }

    #[test]
    fn test_placement_literal_key() {
        let func = GetFieldFunc::new();

        // get_field(col, 'literal') -> leaf-pushable (static field access)
        let args = vec![ExpressionPlacement::Column, ExpressionPlacement::Literal];
        assert_eq!(
            func.placement(&args),
            ExpressionPlacement::MoveTowardsLeafNodes
        );

        // get_field(col, 'a', 'b') -> leaf-pushable (nested static field access)
        let args = vec![
            ExpressionPlacement::Column,
            ExpressionPlacement::Literal,
            ExpressionPlacement::Literal,
        ];
        assert_eq!(
            func.placement(&args),
            ExpressionPlacement::MoveTowardsLeafNodes
        );

        // get_field(get_field(col, 'a'), 'b') represented as MoveTowardsLeafNodes for base
        let args = vec![
            ExpressionPlacement::MoveTowardsLeafNodes,
            ExpressionPlacement::Literal,
        ];
        assert_eq!(
            func.placement(&args),
            ExpressionPlacement::MoveTowardsLeafNodes
        );
    }

    #[test]
    fn test_placement_column_key() {
        let func = GetFieldFunc::new();

        // get_field(col, other_col) -> NOT leaf-pushable (dynamic per-row lookup)
        let args = vec![ExpressionPlacement::Column, ExpressionPlacement::Column];
        assert_eq!(func.placement(&args), ExpressionPlacement::KeepInPlace);

        // get_field(col, 'a', other_col) -> NOT leaf-pushable (dynamic nested lookup)
        let args = vec![
            ExpressionPlacement::Column,
            ExpressionPlacement::Literal,
            ExpressionPlacement::Column,
        ];
        assert_eq!(func.placement(&args), ExpressionPlacement::KeepInPlace);
    }

    #[test]
    fn test_placement_root() {
        let func = GetFieldFunc::new();

        // get_field(root_expr, 'literal') -> NOT leaf-pushable
        let args = vec![
            ExpressionPlacement::KeepInPlace,
            ExpressionPlacement::Literal,
        ];
        assert_eq!(func.placement(&args), ExpressionPlacement::KeepInPlace);

        // get_field(col, root_expr) -> NOT leaf-pushable
        let args = vec![
            ExpressionPlacement::Column,
            ExpressionPlacement::KeepInPlace,
        ];
        assert_eq!(func.placement(&args), ExpressionPlacement::KeepInPlace);
    }

    #[test]
    fn test_placement_edge_cases() {
        let func = GetFieldFunc::new();

        // Empty args -> NOT leaf-pushable
        assert_eq!(func.placement(&[]), ExpressionPlacement::KeepInPlace);

        // Just base, no key -> MoveTowardsLeafNodes (not a valid call but should handle gracefully)
        let args = vec![ExpressionPlacement::Column];
        assert_eq!(
            func.placement(&args),
            ExpressionPlacement::MoveTowardsLeafNodes
        );

        // Literal base with literal key -> NOT leaf-pushable (would be constant-folded)
        let args = vec![ExpressionPlacement::Literal, ExpressionPlacement::Literal];
        assert_eq!(func.placement(&args), ExpressionPlacement::KeepInPlace);
    }
}
