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
use arrow::datatypes::{DataType::Null, Field, FieldRef};
use datafusion_common::utils::{SingleRowListArrayBuilder, list_inner_field_from};
use datafusion_common::{Result, plan_err};
use datafusion_expr::binary::{
    try_type_union_resolution_with_struct, type_union_resolution,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Pick the first non-Null argument's field as the source of element
        // type and metadata; fall back to Null if all inputs are Null.
        // Coercion has already unified element types, so any non-Null input is
        // representative.
        let inner = args
            .arg_fields
            .iter()
            .find(|f| !f.data_type().is_null())
            .map(|f| list_inner_field_from(f))
            .unwrap_or_else(|| Arc::new(Field::new_list_field(Null, true)));
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::List(inner),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let inner_field = match args.return_field.data_type() {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => Some(Arc::clone(field)),
            _ => None,
        };
        make_scalar_function(move |arrays: &[ArrayRef]| {
            make_array_inner_with_field(arrays, inner_field.clone())
        })(&args.args)
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
    make_array_inner_with_field(arrays, None)
}

/// Like [`make_array_inner`] but uses `inner_field` for the resulting list's
/// inner field when supplied, so callers can propagate the input field's
/// metadata onto the constructed `ListArray`'s inner field.
pub(crate) fn make_array_inner_with_field(
    arrays: &[ArrayRef],
    inner_field: Option<FieldRef>,
) -> Result<ArrayRef> {
    let data_type = arrays.iter().find_map(|arg| {
        let arg_type = arg.data_type();
        (!arg_type.is_null()).then_some(arg_type)
    });

    let data_type = data_type.unwrap_or(&Null);
    if data_type.is_null() {
        // Either an empty array or all nulls:
        let length = arrays.iter().map(|a| a.len()).sum();
        let array = new_null_array(&Null, length);
        let mut builder = SingleRowListArrayBuilder::new(array);
        if let Some(field) = inner_field.as_ref() {
            builder = builder.with_field(field);
        }
        Ok(Arc::new(builder.build_list_array()))
    } else {
        match inner_field {
            Some(field) => array_array_with_field::<i32>(arrays, field),
            None => array_array::<i32>(
                arrays,
                data_type.clone(),
                Field::LIST_FIELD_DEFAULT_NAME,
            ),
        }
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
    array_array_with_field::<O>(args, Arc::new(Field::new(field_name, data_type, true)))
}

/// Same as [`array_array`] but takes a fully-formed inner [`FieldRef`] for the
/// resulting list. This lets callers propagate metadata (e.g. Arrow extension
/// type identifiers) onto the produced list's inner field.
pub fn array_array_with_field<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    inner_field: FieldRef,
) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return plan_err!("Array requires at least one argument");
    }

    let data_type = inner_field.data_type();

    let mut data = vec![];
    let mut total_len = 0;
    for arg in args {
        let arg_data = if arg.as_any().is::<NullArray>() {
            ArrayData::new_empty(data_type)
        } else {
            arg.to_data()
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
                mutable.extend(arr_idx, row_idx, row_idx + 1);
            } else {
                mutable.extend_nulls(1);
            }
        }
        offsets.push(O::usize_as(mutable.len()));
    }
    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        inner_field,
        OffsetBuffer::new(offsets.into()),
        arrow::array::make_array(data),
        None,
    )?))
}

pub fn coerce_types_inner(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    if let Ok(unified) = try_type_union_resolution_with_struct(arg_types) {
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
    use arrow::array::Int64Array;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ReturnFieldArgs, ScalarUDFImpl};
    use std::collections::HashMap;

    fn ext_field(data_type: DataType) -> FieldRef {
        let metadata = HashMap::from([(
            "ARROW:extension:name".to_string(),
            "arrow.uuid".to_string(),
        )]);
        Arc::new(Field::new("v", data_type, true).with_metadata(metadata))
    }

    /// Regression test for #21982: `make_array` must propagate the input
    /// field's metadata onto the resulting list's inner field.
    #[test]
    fn make_array_preserves_inner_field_metadata() -> Result<()> {
        let input = ext_field(DataType::Int64);
        let scalar_args: Vec<Option<&ScalarValue>> = vec![None];
        let arg_fields_owned = vec![Arc::clone(&input)];
        let return_field =
            MakeArray::default().return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields_owned,
                scalar_arguments: &scalar_args,
            })?;

        let DataType::List(inner) = return_field.data_type() else {
            panic!("expected List return type");
        };
        assert_eq!(
            inner.metadata().get("ARROW:extension:name"),
            Some(&"arrow.uuid".to_string()),
            "make_array dropped the input field's metadata"
        );

        // Runtime: the produced ListArray's inner field carries the metadata.
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let result = MakeArray::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(arr)],
            arg_fields: vec![Arc::clone(&input)],
            number_rows: 3,
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let array = result.into_array(3)?;
        let DataType::List(rt_inner) = array.data_type() else {
            panic!("runtime output not a List");
        };
        assert_eq!(
            rt_inner.metadata().get("ARROW:extension:name"),
            Some(&"arrow.uuid".to_string())
        );

        Ok(())
    }

    #[test]
    fn make_array_all_null_inputs_have_no_metadata() -> Result<()> {
        let input = Arc::new(Field::new("v", Null, true));
        let null = ScalarValue::Null;
        let scalar_args: Vec<Option<&ScalarValue>> = vec![Some(&null)];
        let arg_fields_owned = vec![Arc::clone(&input)];
        let return_field =
            MakeArray::default().return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields_owned,
                scalar_arguments: &scalar_args,
            })?;
        let DataType::List(inner) = return_field.data_type() else {
            panic!("expected List return type");
        };
        assert!(inner.metadata().is_empty());
        Ok(())
    }
}
