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

// Array Element and Array Slice

use arrow::array::ArrayRef;
use arrow::array::Capacities;
use arrow::array::GenericListArray;
use arrow::array::Int64Array;
use arrow::array::MutableArrayData;
use arrow::array::OffsetSizeTrait;
use arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::cast::as_large_list_array;
use datafusion_common::cast::as_list_array;
use datafusion_common::exec_err;
use datafusion_common::plan_err;
use datafusion_common::DataFusionError;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

use crate::utils::make_scalar_function;

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    ArrayElement,
    array_element,
    array element,
    "extracts the element with the index n from the array.",
    array_element_udf // internal function name
);

#[derive(Debug)]
pub(super) struct ArrayElement {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayElement {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_index(Volatility::Immutable),
            aliases: vec![
                String::from("array_element"),
                String::from("array_extract"),
                String::from("list_element"),
                String::from("list_extract"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayElement {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_element"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        use DataType::*;
        match &arg_types[0] {
            List(field)
            | LargeList(field)
            | FixedSizeList(field, _) => Ok(field.data_type().clone()),
            _ => plan_err!(
                "ArrayElement can only accept List, LargeList or FixedSizeList as the first argument"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_scalar_function(array_element_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// array_element SQL function
///
/// There are two arguments for array_element, the first one is the array, the second one is the 1-indexed index.
/// `array_element(array, index)`
///
/// For example:
/// > array_element(\[1, 2, 3], 2) -> 2
fn array_element_inner(args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_element needs two arguments");
    }

    match &args[0].data_type() {
        DataType::List(_) => {
            let array = as_list_array(&args[0])?;
            let indexes = as_int64_array(&args[1])?;
            general_array_element::<i32>(array, indexes)
        }
        DataType::LargeList(_) => {
            let array = as_large_list_array(&args[0])?;
            let indexes = as_int64_array(&args[1])?;
            general_array_element::<i64>(array, indexes)
        }
        _ => exec_err!(
            "array_element does not support type: {:?}",
            args[0].data_type()
        ),
    }
}

fn general_array_element<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    indexes: &Int64Array,
) -> datafusion_common::Result<ArrayRef>
where
    i64: TryInto<O>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());

    // use_nulls: true, we don't construct List for array_element, so we need explicit nulls.
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], true, capacity);

    fn adjusted_array_index<O: OffsetSizeTrait>(
        index: i64,
        len: O,
    ) -> datafusion_common::Result<Option<O>>
    where
        i64: TryInto<O>,
    {
        let index: O = index.try_into().map_err(|_| {
            DataFusionError::Execution(format!(
                "array_element got invalid index: {}",
                index
            ))
        })?;
        // 0 ~ len - 1
        let adjusted_zero_index = if index < O::usize_as(0) {
            index + len
        } else {
            index - O::usize_as(1)
        };

        if O::usize_as(0) <= adjusted_zero_index && adjusted_zero_index < len {
            Ok(Some(adjusted_zero_index))
        } else {
            // Out of bounds
            Ok(None)
        }
    }

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        let start = offset_window[0];
        let end = offset_window[1];
        let len = end - start;

        // array is null
        if len == O::usize_as(0) {
            mutable.extend_nulls(1);
            continue;
        }

        let index = adjusted_array_index::<O>(indexes.value(row_index), len)?;

        if let Some(index) = index {
            let start = start.as_usize() + index.as_usize();
            mutable.extend(0, start, start + 1_usize);
        } else {
            // Index out of bounds
            mutable.extend_nulls(1);
        }
    }

    let data = mutable.freeze();
    Ok(arrow::array::make_array(data))
}
