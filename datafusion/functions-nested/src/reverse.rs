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

//! [`ScalarUDFImpl`] definitions for array_reverse function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, GenericListArray, MutableArrayData, OffsetSizeTrait,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType::{LargeList, List, Null};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayReverse,
    array_reverse,
    array,
    "reverses the order of elements in the array.",
    array_reverse_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the array with the order of the elements reversed.",
    syntax_example = "array_reverse(array)",
    sql_example = r#"```sql
> select array_reverse([1, 2, 3, 4]);
+------------------------------------------------------------+
| array_reverse(List([1, 2, 3, 4]))                          |
+------------------------------------------------------------+
| [4, 3, 2, 1]                                               |
+------------------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug)]
pub struct ArrayReverse {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayReverse {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayReverse {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: vec!["list_reverse".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayReverse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_reverse"
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
        make_scalar_function(array_reverse_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// array_reverse SQL function
pub fn array_reverse_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    if arg.len() != 1 {
        return exec_err!("array_reverse needs one argument");
    }

    match &arg[0].data_type() {
        List(field) => {
            let array = as_list_array(&arg[0])?;
            general_array_reverse::<i32>(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(&arg[0])?;
            general_array_reverse::<i64>(array, field)
        }
        Null => Ok(Arc::clone(&arg[0])),
        array_type => exec_err!("array_reverse does not support type '{array_type:?}'."),
    }
}

fn general_array_reverse<O: OffsetSizeTrait + TryFrom<i64>>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            offsets.push(offsets[row_index] + O::one());
            mutable.extend(0, 0, 1);
            continue;
        } else {
            nulls.push(true);
        }

        let start = offset_window[0];
        let end = offset_window[1];

        let mut index = end - O::one();
        let mut cnt = 0;

        while index >= start {
            mutable.extend(0, index.to_usize().unwrap(), index.to_usize().unwrap() + 1);
            index = index - O::one();
            cnt += 1;
        }
        offsets.push(offsets[row_index] + O::usize_as(cnt));
    }

    let data = mutable.freeze();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow::array::make_array(data),
        Some(nulls.into()),
    )?))
}
