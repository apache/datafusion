// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`ScalarUDFImpl`] definitions for `make_array` function.

use std::sync::Arc;
use std::vec;

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayData, ArrayRef, Capacities, GenericListArray, MutableArrayData,
    NullArray, OffsetSizeTrait, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef};
use arrow::datatypes::{DataType::Null, Field};
use datafusion_common::metadata::coerced_fields_metadata;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::binary::{try_common_struct_type, type_union_resolution};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_expr_common::sort_properties::ExprProperties;
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
    syntax_example = "make_array([expression1, ..., expression_n])",
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Argument storage types have already been coerced to a common type.
        let element_type = args
            .arg_fields
            .first()
            .map_or(Null, |f| f.data_type().clone());
        let metadata =
            coerced_fields_metadata(args.arg_fields.iter().map(|f| f.as_ref()))?;
        let element = metadata.add_to_field(Field::new_list_field(element_type, true));
        Ok(Field::new(self.name(), DataType::List(Arc::new(element)), true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::List(field) = args.return_type() else {
            return internal_err!("incorrect make_array return type");
        };
        make_scalar_function(|arrays| {
            array_array_with_field::<i32>(arrays, Arc::clone(field))
        })(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn strictly_order_preserving(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        // Not strictly order preserving since null input does not correspond to null output
        Ok(false)
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
    array_array::<i32>(arrays, data_type.clone(), Field::LIST_FIELD_DEFAULT_NAME)
}

/// Convert zero or more [`ArrayRef`] of the same type into a
/// `ListArray` or 'LargeListArray' depending on the offset size.
///
/// Returns empty `ListArray` if input has zero array.
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
///
/// # Public Only for Internal Use:
/// `datafusion-spark` uses this helper to construct arrays with its element field
/// name. This implementation detail is not part of the supported public API.
#[doc(hidden)]
pub fn array_array<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    data_type: DataType,
    field_name: &str,
) -> Result<ArrayRef> {
    array_array_with_field::<O>(args, Arc::new(Field::new(field_name, data_type, true)))
}

fn array_array_with_field<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    field: FieldRef,
) -> Result<ArrayRef> {
    let data_type = field.data_type();
    if args.is_empty() {
        // If input is empty, combined output is an empty array with `Null` data type
        let array = new_null_array(&Null, 0);
        return Ok(Arc::new(
            SingleRowListArrayBuilder::new(array).build_list_array(),
        ));
    }

    let mut data = Vec::with_capacity(args.len());
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
                mutable.try_extend(arr_idx, row_idx, row_idx + 1)?;
            } else {
                mutable.try_extend_nulls(1)?;
            }
        }
        offsets.push(O::usize_as(mutable.len()));
    }
    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        arrow::array::make_array(data),
        None,
    )?))
}

pub fn coerce_types_inner(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    let common_type = try_common_struct_type(arg_types)
        .ok()
        .or_else(|| type_union_resolution(arg_types));
    if let Some(common_type) = common_type {
        Ok(vec![common_type; arg_types.len()])
    } else {
        plan_err!(
            "Failed to unify argument types of {}: [{}]",
            name,
            arg_types.iter().join(", ")
        )
    }
}
