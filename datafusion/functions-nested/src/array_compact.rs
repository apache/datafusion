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

//! [`ScalarUDFImpl`] definitions for array_compact function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, GenericListArray, MutableArrayData, OffsetSizeTrait,
    make_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{LargeList, List, Null};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayCompact,
    array_compact,
    array,
    "removes null values from the array.",
    array_compact_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Removes null values from the array.",
    syntax_example = "array_compact(array)",
    sql_example = r#"```sql
> select array_compact([1, NULL, 2, NULL, 3]) arr;
+-----------+
| arr       |
+-----------+
| [1, 2, 3] |
+-----------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayCompact {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayCompact {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayCompact {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["list_compact".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayCompact {
    fn name(&self) -> &str {
        "array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_compact_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_compact SQL function
fn array_compact_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [input_array] = take_function_args("array_compact", arg)?;

    match &input_array.data_type() {
        List(field) => {
            let array = as_list_array(input_array)?;
            compact_list::<i32>(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(input_array)?;
            compact_list::<i64>(array, field)
        }
        Null => Ok(Arc::clone(input_array)),
        array_type => exec_err!("array_compact does not support type '{array_type}'."),
    }
}

/// Remove null elements from each row of a list array.
fn compact_list<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    field: &Arc<arrow::datatypes::Field>,
) -> Result<ArrayRef> {
    let values = list_array.values();

    // Fast path: no nulls in values, return input unchanged
    if values.null_count() == 0 {
        return Ok(Arc::new(list_array.clone()));
    }

    let original_data = values.to_data();
    let capacity = original_data.len() - values.null_count();
    let mut offsets = Vec::<O>::with_capacity(list_array.len() + 1);
    offsets.push(O::zero());
    let mut mutable = MutableArrayData::with_capacities(
        vec![&original_data],
        false,
        Capacities::Array(capacity),
    );

    for row_index in 0..list_array.len() {
        if list_array.nulls().is_some_and(|n| n.is_null(row_index)) {
            offsets.push(offsets[row_index]);
            continue;
        }

        let start = list_array.offsets()[row_index].as_usize();
        let end = list_array.offsets()[row_index + 1].as_usize();
        let mut copied = 0usize;

        // Batch consecutive non-null elements into single extend() calls
        // to reduce per-element overhead. For [1, 2, NULL, 3, 4] this
        // produces 2 extend calls (0..2, 3..5) instead of 4 individual ones.
        let mut batch_start: Option<usize> = None;
        for i in start..end {
            if values.is_null(i) {
                // Null breaks the current batch — flush it
                if let Some(bs) = batch_start {
                    mutable.extend(0, bs, i);
                    copied += i - bs;
                    batch_start = None;
                }
            } else if batch_start.is_none() {
                batch_start = Some(i);
            }
        }
        // Flush any remaining batch after the loop
        if let Some(bs) = batch_start {
            mutable.extend(0, bs, end);
            copied += end - bs;
        }

        offsets.push(offsets[row_index] + O::usize_as(copied));
    }

    let new_values = make_array(mutable.freeze());
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        new_values,
        list_array.nulls().cloned(),
    )?))
}
