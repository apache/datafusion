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

//! [`ScalarUDFImpl`] definition for array_except function.

use crate::utils::{check_datatypes, make_scalar_function};
use arrow::array::new_null_array;
use arrow::array::{
    Array, ArrayRef, GenericListArray, OffsetSizeTrait, UInt32Array, UInt64Array,
    cast::AsArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{DataType, FieldRef};
use arrow::row::{RowConverter, SortField};
use datafusion_common::utils::{ListCoercion, take_function_args};
use datafusion_common::{HashSet, Result, internal_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use itertools::Itertools;
use std::any::Any;
use std::sync::Arc;

make_udf_expr_and_func!(
    ArrayExcept,
    array_except,
    first_array second_array,
    "returns an array of the elements that appear in the first array but not in the second.",
    array_except_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of the elements that appear in the first array but not in the second.",
    syntax_example = "array_except(array1, array2)",
    sql_example = r#"```sql
> select array_except([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2]                                              |
+----------------------------------------------------+
> select array_except([1, 2, 3, 4], [3, 4, 5, 6]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [3, 4, 5, 6]);           |
+----------------------------------------------------+
| [1, 2]                                              |
+----------------------------------------------------+
```"#,
    argument(
        name = "array1",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "array2",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayExcept {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayExcept {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayExcept {
    pub fn new() -> Self {
        Self {
            signature: Signature::arrays(
                2,
                Some(ListCoercion::FixedSizedListToList),
                Volatility::Immutable,
            ),
            aliases: vec!["list_except".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayExcept {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_except"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (&arg_types[0], &arg_types[1]) {
            (DataType::Null, DataType::Null) => {
                Ok(DataType::new_list(DataType::Null, true))
            }
            (DataType::Null, dt) | (dt, DataType::Null) => Ok(dt.clone()),
            (dt, _) => Ok(dt.clone()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(array_except_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_except_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array1, array2] = take_function_args("array_except", args)?;

    let len = array1.len();
    match (array1.data_type(), array2.data_type()) {
        (DataType::Null, DataType::Null) => Ok(new_null_array(
            &DataType::new_list(DataType::Null, true),
            len,
        )),
        (DataType::Null, dt @ DataType::List(_))
        | (DataType::Null, dt @ DataType::LargeList(_))
        | (dt @ DataType::List(_), DataType::Null)
        | (dt @ DataType::LargeList(_), DataType::Null) => Ok(new_null_array(dt, len)),
        (DataType::List(field), DataType::List(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i32>();
            let list2 = array2.as_list::<i32>();
            let result = general_except::<i32>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (DataType::LargeList(field), DataType::LargeList(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i64>();
            let list2 = array2.as_list::<i64>();
            let result = general_except::<i64>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (dt1, dt2) => {
            internal_err!("array_except got unexpected types: {dt1:?} and {dt2:?}")
        }
    }
}

fn general_except<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<GenericListArray<OffsetSize>> {
    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;

    // Only convert the visible portion of the values array. For sliced
    // ListArrays, values() returns the full underlying array but only
    // elements between the first and last offset are referenced.
    let l_first = l.offsets()[0].as_usize();
    let l_len = l.offsets()[l.len()].as_usize() - l_first;
    let l_values = converter.convert_columns(&[l.values().slice(l_first, l_len)])?;

    let r_first = r.offsets()[0].as_usize();
    let r_len = r.offsets()[r.len()].as_usize() - r_first;
    let r_values = converter.convert_columns(&[r.values().slice(r_first, r_len)])?;

    let mut offsets = Vec::<OffsetSize>::with_capacity(l.len() + 1);
    offsets.push(OffsetSize::usize_as(0));

    let mut indices: Vec<usize> = Vec::with_capacity(l_values.num_rows());
    let mut dedup = HashSet::new();

    let nulls = NullBuffer::union(l.nulls(), r.nulls());

    let l_offsets_iter = l.offsets().iter().tuple_windows();
    let r_offsets_iter = r.offsets().iter().tuple_windows();
    for (list_index, ((l_start, l_end), (r_start, r_end))) in
        l_offsets_iter.zip(r_offsets_iter).enumerate()
    {
        if nulls
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(list_index))
        {
            offsets.push(OffsetSize::usize_as(indices.len()));
            continue;
        }

        for element_index in r_start.as_usize() - r_first..r_end.as_usize() - r_first {
            let right_row = r_values.row(element_index);
            dedup.insert(right_row);
        }
        for element_index in l_start.as_usize() - l_first..l_end.as_usize() - l_first {
            let left_row = l_values.row(element_index);
            if dedup.insert(left_row) {
                indices.push(element_index + l_first);
            }
        }

        offsets.push(OffsetSize::usize_as(indices.len()));
        dedup.clear();
    }

    // Gather distinct left-side values by index.
    // Use UInt64Array for LargeList to support values arrays exceeding u32::MAX.
    let values = if indices.is_empty() {
        arrow::array::new_empty_array(&l.value_type())
    } else if OffsetSize::IS_LARGE {
        let indices =
            UInt64Array::from(indices.into_iter().map(|i| i as u64).collect::<Vec<_>>());
        take(l.values().as_ref(), &indices, None)?
    } else {
        let indices =
            UInt32Array::from(indices.into_iter().map(|i| i as u32).collect::<Vec<_>>());
        take(l.values().as_ref(), &indices, None)?
    };

    Ok(GenericListArray::<OffsetSize>::new(
        field.to_owned(),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    ))
}

#[cfg(test)]
mod tests {
    use super::ArrayExcept;
    use arrow::array::{Array, AsArray, Int32Array, ListArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_common::{Result, config::ConfigOptions};
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_array_except_sliced_lists() -> Result<()> {
        // l: [[1,2], [3,4], [5,6], [7,8]]  →  slice(1,2)  →  [[3,4], [5,6]]
        // r: [[3],   [5],   [6],   [8]]    →  slice(1,2)  →  [[5],   [6]]
        // except(l, r) should be [[3,4], [5]]
        let l_full = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(5), Some(6)]),
            Some(vec![Some(7), Some(8)]),
        ]);
        let r_full = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3)]),
            Some(vec![Some(5)]),
            Some(vec![Some(6)]),
            Some(vec![Some(8)]),
        ]);
        let l_sliced = l_full.slice(1, 2);
        let r_sliced = r_full.slice(1, 2);

        let list_field = Arc::new(Field::new("item", l_sliced.data_type().clone(), true));
        let return_field =
            Arc::new(Field::new("return", l_sliced.data_type().clone(), true));

        let result = ArrayExcept::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(l_sliced)),
                ColumnarValue::Array(Arc::new(r_sliced)),
            ],
            arg_fields: vec![Arc::clone(&list_field), Arc::clone(&list_field)],
            number_rows: 2,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let output = result.into_array(2)?;
        let output = output.as_list::<i32>();

        // Row 0: [3,4] except [5] = [3,4]
        let row0 = output.value(0);
        let row0 = row0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row0.values().as_ref(), &[3, 4]);

        // Row 1: [5,6] except [6] = [5]
        let row1 = output.value(1);
        let row1 = row1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row1.values().as_ref(), &[5]);

        Ok(())
    }
}
