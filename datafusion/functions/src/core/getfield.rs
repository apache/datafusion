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

use arrow::array::{
    make_array, make_comparator, Array, BooleanArray, Capacities, MutableArrayData,
    Scalar,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow_buffer::NullBuffer;
use datafusion_common::cast::{as_map_array, as_struct_array};
use datafusion_common::{
    exec_err, internal_err, plan_datafusion_err, utils::take_function_args, Result,
    ScalarValue,
};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Other Functions"),
    description = r#"Returns a field within a map or a struct with the given key.
    Note: most users invoke `get_field` indirectly via field access
    syntax such as `my_struct_col['field_name']` which results in a call to
    `get_field(my_struct_col, 'field_name')`."#,
    syntax_example = "get_field(expression1, expression2)",
    sql_example = r#"```sql
> create table t (idx varchar, v varchar) as values ('data','fusion'), ('apache', 'arrow');
> select struct(idx, v) from t as c;
+-------------------------+
| struct(c.idx,c.v)       |
+-------------------------+
| {c0: data, c1: fusion}  |
| {c0: apache, c1: arrow} |
+-------------------------+
> select get_field((select struct(idx, v) from t), 'c0');
+-----------------------+
| struct(t.idx,t.v)[c0] |
+-----------------------+
| data                  |
| apache                |
+-----------------------+
> select get_field((select struct(idx, v) from t), 'c1');
+-----------------------+
| struct(t.idx,t.v)[c1] |
+-----------------------+
| fusion                |
| arrow                 |
+-----------------------+
```"#,
    argument(
        name = "expression1",
        description = "The map or struct to retrieve a field for."
    ),
    argument(
        name = "expression2",
        description = "The field name in the map or struct to retrieve data for. Must evaluate to a string."
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

impl GetFieldFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
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
        let [base, field_name] = take_function_args(self.name(), args)?;

        let name = match field_name {
            Expr::Literal(name, _) => name.to_string(),
            other => other.schema_name().to_string(),
        };

        Ok(format!("{base}[{name}]"))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [base, field_name] = take_function_args(self.name(), args)?;
        let name = match field_name {
            Expr::Literal(name, _) => name.to_string(),
            other => other.schema_name().to_string(),
        };

        Ok(format!("{}[{}]", base.schema_name(), name))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Length check handled in the signature
        debug_assert_eq!(args.scalar_arguments.len(), 2);

        match (&args.arg_fields[0].data_type(), args.scalar_arguments[1].as_ref()) {
            (DataType::Map(fields, _), _) => {
                match fields.data_type() {
                    DataType::Struct(fields) if fields.len() == 2 => {
                        // Arrow's MapArray is essentially a ListArray of structs with two columns. They are
                        // often named "key", and "value", but we don't require any specific naming here;
                        // instead, we assume that the second column is the "value" column both here and in
                        // execution.
                        let value_field = fields.get(1).expect("fields should have exactly two members");

                        Ok(value_field.as_ref().clone().with_nullable(true).into())
                    },
                    _ => exec_err!("Map fields must contain a Struct with exactly 2 fields"),
                }
            }
            (DataType::Struct(fields),sv) => {
                sv.and_then(|sv| sv.try_as_str().flatten().filter(|s| !s.is_empty()))
                .map_or_else(
                    || exec_err!("Field name must be a non-empty string"),
                    |field_name| {
                    fields.iter().find(|f| f.name() == field_name)
                    .ok_or(plan_datafusion_err!("Field {field_name} not found in struct"))
                    .map(|f| {
                        let mut child_field = f.as_ref().clone();

                        // If the parent is nullable, then getting the child must be nullable,
                        // so potentially override the return value

                        if args.arg_fields[0].is_nullable() {
                            child_field = child_field.with_nullable(true);
                        }
                        Arc::new(child_field)
                    })
                })
            },
            (DataType::Null, _) => Ok(Field::new(self.name(), DataType::Null, true).into()),
            (other, _) => exec_err!("The expression to get an indexed field is only valid for `Struct`, `Map` or `Null` types, got {other}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [base, field_name] = take_function_args(self.name(), args.args)?;

        if base.data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }

        let arrays =
            ColumnarValue::values_to_arrays(&[base.clone(), field_name.clone()])?;
        let array = Arc::clone(&arrays[0]);
        let name = match field_name {
            ColumnarValue::Scalar(name) => name,
            _ => {
                return exec_err!(
                    "get_field function requires the argument field_name to be a string"
                );
            }
        };

        fn process_map_array(
            array: &Arc<dyn Array>,
            key_array: Arc<dyn Array>,
        ) -> Result<ColumnarValue> {
            let map_array = as_map_array(array.as_ref())?;
            let keys = if key_array.data_type().is_nested() {
                let comparator = make_comparator(
                    map_array.keys().as_ref(),
                    key_array.as_ref(),
                    SortOptions::default(),
                )?;
                let len = map_array.keys().len().min(key_array.len());
                let values = (0..len).map(|i| comparator(i, i).is_eq()).collect();
                let nulls =
                    NullBuffer::union(map_array.keys().nulls(), key_array.nulls());
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

        fn process_map_with_nested_key(
            array: &Arc<dyn Array>,
            key_array: &Arc<dyn Array>,
        ) -> Result<ColumnarValue> {
            let map_array = as_map_array(array.as_ref())?;

            let comparator = make_comparator(
                map_array.keys().as_ref(),
                key_array.as_ref(),
                SortOptions::default(),
            )?;

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

        match (array.data_type(), name) {
            (DataType::Map(_, _), ScalarValue::List(arr)) => {
                let key_array: Arc<dyn Array> = arr;
                process_map_array(&array, key_array)
            }
            (DataType::Map(_, _), ScalarValue::Struct(arr)) => {
                process_map_array(&array, arr as Arc<dyn Array>)
            }
            (DataType::Map(_, _), other) => {
                let data_type = other.data_type();
                if data_type.is_nested() {
                    process_map_with_nested_key(&array, &other.to_array()?)
                } else {
                    process_map_array(&array, other.to_array()?)
                }
            }
            (DataType::Struct(_), ScalarValue::Utf8(Some(k))) => {
                let as_struct_array = as_struct_array(&array)?;
                match as_struct_array.column_by_name(&k) {
                    None => exec_err!("get indexed field {k} not found in struct"),
                    Some(col) => Ok(ColumnarValue::Array(Arc::clone(col))),
                }
            }
            (DataType::Struct(_), name) => exec_err!(
                "get_field is only possible on struct with utf8 indexes. \
                             Received with {name:?} index"
            ),
            (DataType::Null, _) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
            (dt, name) => exec_err!(
                "get_field is only possible on maps with utf8 indexes or struct \
                                         with utf8 indexes. Received {dt} with {name:?} index"
            ),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
