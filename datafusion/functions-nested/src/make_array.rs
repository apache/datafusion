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

//! [`ScalarUDFImpl`] definitions for `make_array` function.

use std::sync::Arc;
use std::vec;

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData,
    NullArray, OffsetSizeTrait, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::{DataType::Null, Field};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, plan_err};
use datafusion_expr::binary::{
    try_type_union_resolution_with_struct, type_union_resolution,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use itertools::Itertools as _;

make_udf_expr_and_func!(
    MakeArray,
    make_array,
    "Returns an Arrow array using the specified input expressions.",
    make_array_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array using the specified input expressions.",
    syntax_example = "make_array(expression1[, ..., expression_n])",
    sql_example = r#"```sql
> select make_array(1, 2, 3, 4, 5);
+----------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)) |
+----------------------------------------------------------+
| [1, 2, 3, 4, 5]                                          |
+----------------------------------------------------------+
```"#,
    argument(
        name = "expression_n",
        description = "Expression to include in the output array. Can be a constant, column, or function, and any combination of arithmetic or string operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MakeArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for MakeArray {
    fn default() -> Self {
        Self::new()
    }
}

impl MakeArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("make_list")],
        }
    }
}

impl ScalarUDFImpl for MakeArray {
    fn name(&self) -> &str {
        "make_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element_type = if arg_types.is_empty() {
            Null
        } else {
            // At this point, all the type in array should be coerced to the same one.
            arg_types[0].to_owned()
        };

        Ok(DataType::new_list(element_type, true))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(make_array_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(vec![])
        } else {
            coerce_types_inner(arg_types, self.name())
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// `make_array_inner` is the implementation of the `make_array` function.
/// Constructs an array using the input `data` as `ArrayRef`.
/// Returns a reference-counted `Array` instance result.
pub(crate) fn make_array_inner(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let data_type = arrays.iter().find_map(|arg| {
        let arg_type = arg.data_type();
        (!arg_type.is_null()).then_some(arg_type)
    });

    let data_type = data_type.unwrap_or(&Null);
    if data_type.is_null() {
        // Either an empty array or all nulls:
        let length = arrays.iter().map(|a| a.len()).sum();
        let array = new_null_array(&Null, length);
        Ok(Arc::new(
            SingleRowListArrayBuilder::new(array).build_list_array(),
        ))
    } else {
        array_array::<i32>(arrays, data_type.clone(), Field::LIST_FIELD_DEFAULT_NAME)
    }
}

/// Convert one or more [`ArrayRef`] of the same type into a
/// `ListArray` or 'LargeListArray' depending on the offset size.
///
/// # Example (non nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are non nested
/// would return a single new `ListArray`, where each row was a list
/// of 2 elements:
///
/// ```text
/// ┌─────────┐   ┌─────────┐           ┌──────────────┐
/// │ ┌─────┐ │   │ ┌─────┐ │           │ ┌──────────┐ │
/// │ │  A  │ │   │ │  X  │ │           │ │  [A, X]  │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │NULL │ │   │ │  Y  │ │──────────▶│ │[NULL, Y] │ │
/// │ ├─────┤ │   │ ├─────┤ │           │ ├──────────┤ │
/// │ │  C  │ │   │ │  Z  │ │           │ │  [C, Z]  │ │
/// │ └─────┘ │   │ └─────┘ │           │ └──────────┘ │
/// └─────────┘   └─────────┘           └──────────────┘
///   col1           col2                    output
/// ```
///
/// # Example (nested)
///
/// Calling `array(col1, col2)` where col1 and col2 are lists
/// would return a single new `ListArray`, where each row was a list
/// of the corresponding elements of col1 and col2.
///
/// ``` text
/// ┌──────────────┐   ┌──────────────┐        ┌─────────────────────────────┐
/// │ ┌──────────┐ │   │ ┌──────────┐ │        │ ┌────────────────────────┐  │
/// │ │  [A, X]  │ │   │ │    []    │ │        │ │    [[A, X], []]        │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────┤  │
/// │ │[NULL, Y] │ │   │ │[Q, R, S] │ │───────▶│ │ [[NULL, Y], [Q, R, S]] │  │
/// │ ├──────────┤ │   │ ├──────────┤ │        │ ├────────────────────────│  │
/// │ │  [C, Z]  │ │   │ │   NULL   │ │        │ │    [[C, Z], NULL]      │  │
/// │ └──────────┘ │   │ └──────────┘ │        │ └────────────────────────┘  │
/// └──────────────┘   └──────────────┘        └─────────────────────────────┘
///      col1               col2                         output
/// ```
pub fn array_array<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    data_type: DataType,
    field_name: &str,
) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return plan_err!("Array requires at least one argument");
    }

    // Widen the element type so inputs that differ only in nested-field
    // nullability (e.g. a struct field that is non-nullable in one argument and
    // nullable in another) can be combined. Without this, `MutableArrayData`
    // below panics with "Arrays with inconsistent types". See issue #22366.
    let data_type = merge_nullability(
        std::iter::once(data_type.clone())
            .chain(args.iter().map(|arg| arg.data_type().clone())),
    )
    .unwrap_or(data_type);

    let mut data = vec![];
    let mut total_len = 0;
    for arg in args {
        let arg_data = if arg.as_any().is::<NullArray>() {
            ArrayData::new_empty(&data_type)
        } else if arg.data_type() == &data_type {
            arg.to_data()
        } else {
            // Only nullability metadata differs; this cast widens it cheaply.
            arrow::compute::cast(arg, &data_type)?.to_data()
        };
        total_len += arg_data.len();
        data.push(arg_data);
    }

    let mut offsets: Vec<O> = Vec::with_capacity(total_len);
    offsets.push(O::usize_as(0));

    let capacity = Capacities::Array(total_len);
    let data_ref = data.iter().collect::<Vec<_>>();
    let mut mutable = MutableArrayData::with_capacities(data_ref, true, capacity);

    let num_rows = args[0].len();
    for row_idx in 0..num_rows {
        for (arr_idx, arg) in args.iter().enumerate() {
            if !arg.as_any().is::<NullArray>()
                && !arg.is_null(row_idx)
                && arg.is_valid(row_idx)
            {
                mutable.try_extend(arr_idx, row_idx, row_idx + 1)?;
            } else {
                mutable.try_extend_nulls(1)?;
            }
        }
        offsets.push(O::usize_as(mutable.len()));
    }
    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new(field_name, data_type, true)),
        OffsetBuffer::new(offsets.into()),
        arrow::array::make_array(data),
        None,
    )?))
}

/// Merge `data_types` into a single type, OR-ing the nullable flag at every
/// nesting level (struct fields, list elements, ...). `Null` inputs are
/// ignored.
///
/// Returns `None` when there is no non-null input or the inputs are
/// structurally incompatible (different field names, element types, ...), in
/// which case callers fall back to their previous behavior.
///
/// This is what allows e.g. `make_array(named_struct('i', 1), some_column)` to
/// succeed when the literal yields a non-nullable field and the column yields a
/// nullable one: the result widens both to nullable. See issue #22366.
fn merge_nullability(data_types: impl IntoIterator<Item = DataType>) -> Option<DataType> {
    let mut merged: Option<Field> = None;
    for data_type in data_types {
        if data_type.is_null() {
            continue;
        }
        let field = Field::new(Field::LIST_FIELD_DEFAULT_NAME, data_type, true);
        match merged {
            None => merged = Some(field),
            // `Field::try_merge` ORs nullability and recurses into nested types;
            // it errors only when the structures are incompatible.
            Some(ref mut m) => m.try_merge(&field).ok()?,
        }
    }
    merged.map(|f| f.data_type().clone())
}

pub fn coerce_types_inner(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    if let Ok(unified) = try_type_union_resolution_with_struct(arg_types) {
        // `try_type_union_resolution_with_struct` coerces the inner field types
        // but preserves each argument's own nested-field nullability, so the
        // returned struct types can still differ in nullability. Widen them to
        // a single common type, both so the declared return type matches the
        // value produced at runtime and so the arguments can actually be
        // combined. See issue #22366.
        if let Some(merged) = merge_nullability(unified.iter().cloned()) {
            return Ok(vec![merged; unified.len()]);
        }
        return Ok(unified);
    }

    if let Some(unified) = type_union_resolution(arg_types) {
        Ok(vec![unified; arg_types.len()])
    } else {
        plan_err!(
            "Failed to unify argument types of {}: [{}]",
            name,
            arg_types.iter().join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StructArray};
    use arrow::datatypes::Fields;

    /// Builds a single-row struct array `{ i: 1 }` whose `i` field has the
    /// given nullability flag. The underlying data is identical regardless of
    /// the flag.
    fn struct_array_with_field_nullable(nullable: bool) -> ArrayRef {
        let field = Arc::new(Field::new("i", DataType::Int32, nullable));
        let values = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        Arc::new(StructArray::new(
            Fields::from(vec![field]),
            vec![values],
            None,
        ))
    }

    /// Reproduces https://github.com/apache/datafusion/issues/22366:
    /// `make_array` over structs that differ only in nested-field nullability
    /// must succeed (widening nullable flags) rather than panic in
    /// `MutableArrayData`.
    #[test]
    fn make_array_relaxes_nested_field_nullability() {
        let non_nullable = struct_array_with_field_nullable(false);
        let nullable = struct_array_with_field_nullable(true);

        let result = make_array_inner(&[non_nullable, nullable]).expect(
            "make_array should accept structs differing only in field nullability",
        );

        // The result is a single-row list of two struct elements.
        let list = result
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .expect("expected a List array");
        assert_eq!(list.len(), 1);
        assert_eq!(list.value(0).len(), 2);
    }
}
