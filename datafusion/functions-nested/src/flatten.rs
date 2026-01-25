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

//! [`ScalarUDFImpl`] definitions for flatten function.

use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{
    DataType,
    DataType::{FixedSizeList, LargeList, List, Null},
};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    Flatten,
    flatten,
    array,
    "flattens an array of arrays into a single array.",
    flatten_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Converts an array of arrays to a flat array.\n\n- Applies to any depth of nested arrays\n- Does not change arrays that are already flat\n\nThe flattened array contains all the elements from all source arrays.",
    syntax_example = "flatten(array)",
    sql_example = r#"```sql
> select flatten([[1, 2], [3, 4]]);
+------------------------------+
| flatten(List([1,2], [3,4]))  |
+------------------------------+
| [1, 2, 3, 4]                 |
+------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Flatten {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for Flatten {
    fn default() -> Self {
        Self::new()
    }
}

impl Flatten {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for Flatten {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "flatten"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let data_type = match &arg_types[0] {
            List(field) => match field.data_type() {
                List(field) | FixedSizeList(field, _) => List(Arc::clone(field)),
                LargeList(field) => LargeList(Arc::clone(field)),
                _ => arg_types[0].clone(),
            },
            LargeList(field) => match field.data_type() {
                List(field) | LargeList(field) | FixedSizeList(field, _) => {
                    LargeList(Arc::clone(field))
                }
                _ => arg_types[0].clone(),
            },
            Null => Null,
            _ => exec_err!(
                "Not reachable, data_type should be List, LargeList or FixedSizeList"
            )?,
        };

        Ok(data_type)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(flatten_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn flatten_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("flatten", args)?;

    match array.data_type() {
        List(_) => {
            let (_field, offsets, values, nulls) =
                as_list_array(&array)?.clone().into_parts();
            let values = cast_fsl_to_list(values)?;

            match values.data_type() {
                List(_) => {
                    let (inner_field, inner_offsets, inner_values, _) =
                        as_list_array(&values)?.clone().into_parts();
                    let offsets =
                        get_offsets_for_flatten::<i32, i32>(inner_offsets, &offsets);
                    let flattened_array = GenericListArray::<i32>::new(
                        inner_field,
                        offsets,
                        inner_values,
                        nulls,
                    );

                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                LargeList(_) => {
                    let (inner_field, inner_offsets, inner_values, _) =
                        as_large_list_array(&values)?.clone().into_parts();
                    let offsets =
                        get_offsets_for_flatten::<i64, i32>(inner_offsets, &offsets);
                    let flattened_array = GenericListArray::<i64>::new(
                        inner_field,
                        offsets,
                        inner_values,
                        nulls,
                    );
                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                _ => Ok(Arc::clone(array) as ArrayRef),
            }
        }
        LargeList(_) => {
            let (_field, offsets, values, nulls) =
                as_large_list_array(&array)?.clone().into_parts();
            let values = cast_fsl_to_list(values)?;

            match values.data_type() {
                List(_) => {
                    let (inner_field, inner_offsets, inner_values, _) =
                        as_list_array(&values)?.clone().into_parts();
                    let offsets = get_large_offsets_for_flatten(inner_offsets, &offsets);
                    let flattened_array = GenericListArray::<i64>::new(
                        inner_field,
                        offsets,
                        inner_values,
                        nulls,
                    );

                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                LargeList(_) => {
                    let (inner_field, inner_offsets, inner_values, _) =
                        as_large_list_array(&values)?.clone().into_parts();
                    let offsets =
                        get_offsets_for_flatten::<i64, i64>(inner_offsets, &offsets);
                    let flattened_array = GenericListArray::<i64>::new(
                        inner_field,
                        offsets,
                        inner_values,
                        nulls,
                    );

                    Ok(Arc::new(flattened_array) as ArrayRef)
                }
                _ => Ok(Arc::clone(array) as ArrayRef),
            }
        }
        Null => Ok(Arc::clone(array)),
        _ => {
            exec_err!("flatten does not support type '{}'", array.data_type())
        }
    }
}

// Create new offsets that are equivalent to `flatten` the array.
fn get_offsets_for_flatten<O: OffsetSizeTrait, P: OffsetSizeTrait>(
    inner_offsets: OffsetBuffer<O>,
    outer_offsets: &OffsetBuffer<P>,
) -> OffsetBuffer<O> {
    let buffer = inner_offsets.into_inner();
    let offsets: Vec<O> = outer_offsets
        .iter()
        .map(|i| buffer[i.to_usize().unwrap()])
        .collect();
    OffsetBuffer::new(offsets.into())
}

// Create new large offsets that are equivalent to `flatten` the array.
fn get_large_offsets_for_flatten<O: OffsetSizeTrait, P: OffsetSizeTrait>(
    inner_offsets: OffsetBuffer<O>,
    outer_offsets: &OffsetBuffer<P>,
) -> OffsetBuffer<i64> {
    let buffer = inner_offsets.into_inner();
    let offsets: Vec<i64> = outer_offsets
        .iter()
        .map(|i| buffer[i.to_usize().unwrap()].to_i64().unwrap())
        .collect();
    OffsetBuffer::new(offsets.into())
}

fn cast_fsl_to_list(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        FixedSizeList(field, _) => {
            Ok(arrow::compute::cast(&array, &List(Arc::clone(field)))?)
        }
        _ => Ok(array),
    }
}
