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

//! [`ScalarUDFImpl`] definitions for `array_append`, `array_prepend` and `array_concat` functions.

use std::sync::Arc;
use std::{any::Any, cmp::Ordering};

use arrow::array::{
    Array, ArrayRef, Capacities, GenericListArray, MutableArrayData, NullBufferBuilder,
    OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::ListCoercion;
use datafusion_common::Result;
use datafusion_common::{
    cast::as_generic_list_array,
    exec_err, not_impl_err, plan_err,
    utils::{list_ndims, take_function_args},
};
use datafusion_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, ColumnarValue, Documentation,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_macros::user_doc;

use crate::utils::{align_array_dimensions, check_datatypes, make_scalar_function};

make_udf_expr_and_func!(
    ArrayAppend,
    array_append,
    array element,                                // arg name
    "appends an element to the end of an array.", // doc
    array_append_udf                              // internal function name
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Appends an element to the end of an array.",
    syntax_example = "array_append(array, element)",
    sql_example = r#"```sql
> select array_append([1, 2, 3], 4);
+--------------------------------------+
| array_append(List([1,2,3]),Int64(4)) |
+--------------------------------------+
| [1, 2, 3, 4]                         |
+--------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "element", description = "Element to append to the array.")
)]
#[derive(Debug)]
pub struct ArrayAppend {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayAppend {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAppend {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![
                String::from("list_append"),
                String::from("array_push_back"),
                String::from("list_push_back"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayAppend {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_append"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_append_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

make_udf_expr_and_func!(
    ArrayPrepend,
    array_prepend,
    element array,
    "Prepends an element to the beginning of an array.",
    array_prepend_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Prepends an element to the beginning of an array.",
    syntax_example = "array_prepend(element, array)",
    sql_example = r#"```sql
> select array_prepend(1, [2, 3, 4]);
+---------------------------------------+
| array_prepend(Int64(1),List([2,3,4])) |
+---------------------------------------+
| [1, 2, 3, 4]                          |
+---------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "element", description = "Element to prepend to the array.")
)]
#[derive(Debug)]
pub struct ArrayPrepend {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayPrepend {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayPrepend {
    pub fn new() -> Self {
        Self {
            signature: Signature {
                type_signature: TypeSignature::ArraySignature(
                    ArrayFunctionSignature::Array {
                        arguments: vec![
                            ArrayFunctionArgument::Element,
                            ArrayFunctionArgument::Array,
                        ],
                        array_coercion: Some(ListCoercion::FixedSizedListToList),
                    },
                ),
                volatility: Volatility::Immutable,
            },
            aliases: vec![
                String::from("list_prepend"),
                String::from("array_push_front"),
                String::from("list_push_front"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayPrepend {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_prepend"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_prepend_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

make_udf_expr_and_func!(
    ArrayConcat,
    array_concat,
    "Concatenates arrays.",
    array_concat_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Concatenates arrays.",
    syntax_example = "array_concat(array[, ..., array_n])",
    sql_example = r#"```sql
> select array_concat([1, 2], [3, 4], [5, 6]);
+---------------------------------------------------+
| array_concat(List([1,2]),List([3,4]),List([5,6])) |
+---------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                |
+---------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "array_n",
        description = "Subsequent array column or literal array to concatenate."
    )
)]
#[derive(Debug)]
pub struct ArrayConcat {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayConcat {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayConcat {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![
                String::from("array_cat"),
                String::from("list_concat"),
                String::from("list_cat"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayConcat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_concat"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let mut expr_type = DataType::Null;
        let mut max_dims = 0;
        for arg_type in arg_types {
            let DataType::List(field) = arg_type else {
                return plan_err!(
                    "The array_concat function can only accept list as the args."
                );
            };
            if !field.data_type().equals_datatype(&DataType::Null) {
                let dims = list_ndims(arg_type);
                expr_type = match max_dims.cmp(&dims) {
                    Ordering::Greater => expr_type,
                    Ordering::Equal => {
                        if expr_type == DataType::Null {
                            arg_type.clone()
                        } else if !expr_type.equals_datatype(arg_type) {
                            return plan_err!(
                            "It is not possible to concatenate arrays of different types. Expected: {}, got: {}", expr_type, arg_type
                                );
                        } else {
                            expr_type
                        }
                    }

                    Ordering::Less => {
                        max_dims = dims;
                        arg_type.clone()
                    }
                };
            }
        }

        Ok(expr_type)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_concat_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Array_concat/Array_cat SQL function
pub(crate) fn array_concat_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        return exec_err!("array_concat expects at least one arguments");
    }

    let mut new_args = vec![];
    for arg in args {
        let ndim = list_ndims(arg.data_type());
        let base_type = datafusion_common::utils::base_type(arg.data_type());
        if ndim == 0 {
            return not_impl_err!("Array is not type '{base_type:?}'.");
        }
        if !base_type.eq(&DataType::Null) {
            new_args.push(Arc::clone(arg));
        }
    }

    match &args[0].data_type() {
        DataType::LargeList(_) => concat_internal::<i64>(new_args.as_slice()),
        _ => concat_internal::<i32>(new_args.as_slice()),
    }
}

fn concat_internal<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args = align_array_dimensions::<O>(args.to_vec())?;

    let list_arrays = args
        .iter()
        .map(|arg| as_generic_list_array::<O>(arg))
        .collect::<Result<Vec<_>>>()?;
    // Assume number of rows is the same for all arrays
    let row_count = list_arrays[0].len();

    let mut array_lengths = vec![];
    let mut arrays = vec![];
    let mut valid = NullBufferBuilder::new(row_count);
    for i in 0..row_count {
        let nulls = list_arrays
            .iter()
            .map(|arr| arr.is_null(i))
            .collect::<Vec<_>>();

        // If all the arrays are null, the concatenated array is null
        let is_null = nulls.iter().all(|&x| x);
        if is_null {
            array_lengths.push(0);
            valid.append_null();
        } else {
            // Get all the arrays on i-th row
            let values = list_arrays
                .iter()
                .map(|arr| arr.value(i))
                .collect::<Vec<_>>();

            let elements = values
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<&dyn Array>>();

            // Concatenated array on i-th row
            let concatenated_array = arrow::compute::concat(elements.as_slice())?;
            array_lengths.push(concatenated_array.len());
            arrays.push(concatenated_array);
            valid.append_non_null();
        }
    }
    // Assume all arrays have the same data type
    let data_type = list_arrays[0].value_type();

    let elements = arrays
        .iter()
        .map(|a| a.as_ref())
        .collect::<Vec<&dyn Array>>();

    let list_arr = GenericListArray::<O>::new(
        Arc::new(Field::new_list_field(data_type, true)),
        OffsetBuffer::from_lengths(array_lengths),
        Arc::new(arrow::compute::concat(elements.as_slice())?),
        valid.finish(),
    );

    Ok(Arc::new(list_arr))
}

// Kernel functions

/// Array_append SQL function
pub(crate) fn array_append_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array, _] = take_function_args("array_append", args)?;

    match array.data_type() {
        DataType::LargeList(_) => general_append_and_prepend::<i64>(args, true),
        _ => general_append_and_prepend::<i32>(args, true),
    }
}

/// Array_prepend SQL function
pub(crate) fn array_prepend_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [_, array] = take_function_args("array_prepend", args)?;

    match array.data_type() {
        DataType::LargeList(_) => general_append_and_prepend::<i64>(args, false),
        _ => general_append_and_prepend::<i32>(args, false),
    }
}

fn general_append_and_prepend<O: OffsetSizeTrait>(
    args: &[ArrayRef],
    is_append: bool,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let (list_array, element_array) = if is_append {
        let list_array = as_generic_list_array::<O>(&args[0])?;
        let element_array = &args[1];
        check_datatypes("array_append", &[element_array, list_array.values()])?;
        (list_array, element_array)
    } else {
        let list_array = as_generic_list_array::<O>(&args[1])?;
        let element_array = &args[0];
        check_datatypes("array_prepend", &[list_array.values(), element_array])?;
        (list_array, element_array)
    };

    let res = match list_array.value_type() {
        DataType::List(_) => concat_internal::<O>(args)?,
        DataType::LargeList(_) => concat_internal::<O>(args)?,
        data_type => {
            return generic_append_and_prepend::<O>(
                list_array,
                element_array,
                &data_type,
                is_append,
            );
        }
    };

    Ok(res)
}

/// Appends or prepends elements to a ListArray.
///
/// This function takes a ListArray, an ArrayRef, a FieldRef, and a boolean flag
/// indicating whether to append or prepend the elements. It returns a `Result<ArrayRef>`
/// representing the resulting ListArray after the operation.
///
/// # Arguments
///
/// * `list_array` - A reference to the ListArray to which elements will be appended/prepended.
/// * `element_array` - A reference to the Array containing elements to be appended/prepended.
/// * `field` - A reference to the Field describing the data type of the arrays.
/// * `is_append` - A boolean flag indicating whether to append (`true`) or prepend (`false`) elements.
///
/// # Examples
///
/// generic_append_and_prepend(
///     [1, 2, 3], 4, append => [1, 2, 3, 4]
///     5, [6, 7, 8], prepend => [5, 6, 7, 8]
/// )
fn generic_append_and_prepend<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    element_array: &ArrayRef,
    data_type: &DataType,
    is_append: bool,
) -> Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let mut offsets = vec![O::usize_as(0)];
    let values = list_array.values();
    let original_data = values.to_data();
    let element_data = element_array.to_data();
    let capacity = Capacities::Array(original_data.len() + element_data.len());

    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data, &element_data],
        false,
        capacity,
    );

    let values_index = 0;
    let element_index = 1;

    for (row_index, offset_window) in list_array.offsets().windows(2).enumerate() {
        let start = offset_window[0].to_usize().unwrap();
        let end = offset_window[1].to_usize().unwrap();
        if is_append {
            mutable.extend(values_index, start, end);
            mutable.extend(element_index, row_index, row_index + 1);
        } else {
            mutable.extend(element_index, row_index, row_index + 1);
            mutable.extend(values_index, start, end);
        }
        offsets.push(offsets[row_index] + O::usize_as(end - start + 1));
    }

    let data = mutable.freeze();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::new(Field::new_list_field(data_type.to_owned(), true)),
        OffsetBuffer::new(offsets.into()),
        arrow::array::make_array(data),
        None,
    )?))
}
