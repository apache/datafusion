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

//! [`datafusion_expr::HigherOrderUDF`] definitions for map_transform function.

use arrow::{
    array::{Array, AsArray, MapArray, StructArray},
    buffer::OffsetBuffer,
    compute::take_arrays,
    datatypes::{DataType, Field, FieldRef, Fields},
};
use datafusion_common::{
    Result, ScalarValue, exec_err, plan_err,
    utils::{list_values_row_number, take_function_args},
};
use datafusion_expr::{
    ColumnarValue, Documentation, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

use crate::utils::get_map_entry_field;

make_higher_order_function_expr_and_func!(
    MapTransform,
    map_transform,
    map lambda,
    "transforms the values of a map",
    map_transform_higher_order_function
);

#[user_doc(
    doc_section(label = "Map Functions"),
    description = "Returns a map that applies the lambda to each entry of the map and \
    transforms the values. The keys are preserved unchanged.",
    syntax_example = "map_transform(map, (k, v) -> expr)",
    sql_example = r#"```sql
> select map_transform(MAP {'a': 1, 'b': 2, 'c': 3}, (k, v) -> v * 10);
+--------------------------------------------------------------+
| map_transform(MAP {'a': 1, 'b': 2, 'c': 3}, (k, v) -> v * 10) |
+--------------------------------------------------------------+
| {a: 10, b: 20, c: 30}                                        |
+--------------------------------------------------------------+
```"#,
    argument(
        name = "map",
        description = "Map expression. Can be a constant, column, or function, and any combination of map operators."
    ),
    argument(
        name = "lambda",
        description = "Lambda accepting two parameters `(key, value)`. The return value is used as the new value for the entry."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapTransform {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl Default for MapTransform {
    fn default() -> Self {
        Self::new()
    }
}

impl MapTransform {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::exact(
                vec![ValueOrLambda::Value(()), ValueOrLambda::Lambda(())],
                Volatility::Immutable,
            ),
            aliases: vec![],
        }
    }
}

impl HigherOrderUDFImpl for MapTransform {
    fn name(&self) -> &str {
        "map_transform"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [map_type] = take_function_args(self.name(), arg_types)?;
        match map_type {
            DataType::Map(_, _) => Ok(vec![map_type.clone()]),
            other => plan_err!(
                "{} expected a map as first argument, got {other}",
                self.name()
            ),
        }
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [ValueOrLambda::Value(map), ValueOrLambda::Lambda(_lambda)] =
            take_function_args(self.name(), fields)?
        else {
            return plan_err!("{} expects a map followed by a lambda", self.name());
        };

        let entry_fields = get_map_entry_field(map.data_type())?;
        let key_field = Arc::clone(entry_fields.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "{} expected map entries to contain a key field",
                self.name()
            ))
        })?);
        let value_field = Arc::clone(entry_fields.last().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "{} expected map entries to contain a value field",
                self.name()
            ))
        })?);

        Ok(LambdaParametersProgress::Complete(vec![vec![
            key_field,
            value_field,
        ]]))
    }

    fn return_field_from_args(
        &self,
        args: HigherOrderReturnFieldArgs,
    ) -> Result<Arc<Field>> {
        let [ValueOrLambda::Value(map), ValueOrLambda::Lambda(lambda)] =
            take_function_args(self.name(), args.arg_fields)?
        else {
            return plan_err!("{} expects a map followed by a lambda", self.name());
        };

        let (entries_field, ordered_keys) = match map.data_type() {
            DataType::Map(field, ordered) => (Arc::clone(field), *ordered),
            other => return plan_err!("expected map, got {other}"),
        };

        let entry_fields = match entries_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            other => {
                return plan_err!("expected map entries to be a struct, got {other}");
            }
        };

        let key_field = Arc::clone(entry_fields.first().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "{} expected map entries to contain a key field",
                self.name()
            ))
        })?);

        let original_value_field = entry_fields.last().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "{} expected map entries to contain a value field",
                self.name()
            ))
        })?;

        let new_value_field = Arc::new(Field::new(
            original_value_field.name(),
            lambda.data_type().clone(),
            lambda.is_nullable(),
        ));

        let new_entries_struct =
            DataType::Struct(Fields::from(vec![key_field, new_value_field]));
        let new_entries_field = Arc::new(Field::new(
            entries_field.name(),
            new_entries_struct,
            entries_field.is_nullable(),
        ));

        Ok(Arc::new(Field::new(
            "",
            DataType::Map(new_entries_field, ordered_keys),
            map.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [map, lambda] = take_function_args(self.name(), &args.args)?;
        let (ValueOrLambda::Value(map), ValueOrLambda::Lambda(lambda)) = (map, lambda)
        else {
            return plan_err!("{} expects a map followed by a lambda", self.name());
        };

        let map_array_dyn = map.to_array(args.number_rows)?;
        let map_array = match map_array_dyn.data_type() {
            DataType::Map(_, _) => map_array_dyn.as_map(),
            other => return exec_err!("{} expected a map, got {other}", self.name()),
        };

        // Fast path: every row is null.
        if map_array.null_count() == map_array.len() {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                args.return_type(),
            )?));
        }

        let offsets = map_array.offsets();
        let first = offsets.first().copied().unwrap_or(0) as usize;
        let last = offsets.last().copied().unwrap_or(0) as usize;
        let len = last - first;

        let new_entries_field = match args.return_field.data_type() {
            DataType::Map(field, _) => Arc::clone(field),
            other => {
                return exec_err!(
                    "{} expected return_field to be a map, got {other}",
                    self.name()
                );
            }
        };
        let (new_key_field, new_value_field) = match new_entries_field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                (Arc::clone(&fields[0]), Arc::clone(&fields[1]))
            }
            other => {
                return exec_err!(
                    "{} expected map entries struct with two fields, got {other}",
                    self.name()
                );
            }
        };

        let flat_keys = if first == 0 && last == map_array.keys().len() {
            Arc::clone(map_array.keys())
        } else {
            map_array.keys().slice(first, len)
        };
        let flat_values = if first == 0 && last == map_array.values().len() {
            Arc::clone(map_array.values())
        } else {
            map_array.values().slice(first, len)
        };

        // Fast path: no entries at all and the map has no nulls — return an empty map
        // mirroring the input row count.
        if len == 0 && map_array.null_count() == 0 {
            return Ok(ColumnarValue::Scalar(ScalarValue::new_default(
                args.return_type(),
            )?));
        }

        let keys_param = || Ok(Arc::clone(&flat_keys));
        let values_param = || Ok(Arc::clone(&flat_values));

        let transformed_values = lambda
            .evaluate(&[&keys_param, &values_param], |arrays| {
                let indices = list_values_row_number(&map_array_dyn)?;
                Ok(take_arrays(arrays, &indices, None)?)
            })?
            .into_array(len)?;

        let adjusted_offsets = if first == 0 {
            offsets.clone()
        } else {
            let first_i32 = first as i32;
            let adjusted = offsets.iter().map(|o| *o - first_i32).collect();
            OffsetBuffer::new(adjusted)
        };

        let new_entries = StructArray::try_new(
            Fields::from(vec![new_key_field, new_value_field]),
            vec![Arc::clone(&flat_keys), transformed_values],
            None,
        )?;

        let new_map = MapArray::try_new(
            new_entries_field,
            adjusted_offsets,
            new_entries,
            map_array.nulls().cloned(),
            matches!(map_array_dyn.data_type(), DataType::Map(_, true)),
        )?;

        Ok(ColumnarValue::Array(Arc::new(new_map)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{
            Array, ArrayRef, Int32Array, MapArray, RecordBatch, StringArray, StructArray,
        },
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field, Fields},
    };
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{
        Expr, col,
        execution_props::ExecutionProps,
        expr::{HigherOrderFunction, LambdaVariable},
        lambda, lit,
    };
    use datafusion_physical_expr::create_physical_expr;

    use crate::map_transform::map_transform_higher_order_function;

    fn create_str_int_map(
        keys: Vec<&str>,
        values: Vec<Option<i32>>,
        offsets: OffsetBuffer<i32>,
        nulls: Option<NullBuffer>,
    ) -> MapArray {
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let entries_fields =
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]);

        let keys_array: ArrayRef = Arc::new(StringArray::from(keys));
        let values_array: ArrayRef = Arc::new(Int32Array::from(values));

        let entries = StructArray::new(
            entries_fields.clone(),
            vec![keys_array, values_array],
            None,
        );
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields),
            false,
        ));

        MapArray::new(entries_field, offsets, entries, nulls, false)
    }

    fn eval_map_transform(
        map: MapArray,
        lambda_body: Expr,
        param_names: [&str; 2],
    ) -> Result<ArrayRef> {
        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new("m", map.data_type().clone(), map.is_nullable())].into(),
            HashMap::new(),
        )?;
        let len = map.len();

        create_physical_expr(
            &Expr::HigherOrderFunction(HigherOrderFunction::new(
                map_transform_higher_order_function(),
                vec![col("m"), lambda(param_names, lambda_body)],
            )),
            &schema,
            &ExecutionProps::new(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(map.clone())],
        )?)?
        .into_array(len)
    }

    fn value_var(name: &str) -> Expr {
        Expr::LambdaVariable(LambdaVariable::new(
            name.to_string(),
            Some(Arc::new(Field::new(name, DataType::Int32, true))),
        ))
    }

    #[test]
    fn map_transform_doubles_values() {
        let map = create_str_int_map(
            vec!["a", "b", "c"],
            vec![Some(1), Some(2), Some(3)],
            OffsetBuffer::<i32>::from_lengths(vec![3]),
            None,
        );

        let result =
            eval_map_transform(map, value_var("v") * lit(2i32), ["k", "v"]).unwrap();
        let result_map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let result_values = result_map
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(result_values.values(), &[2, 4, 6]);
    }

    #[test]
    fn map_transform_uses_keys_via_case() {
        let map = create_str_int_map(
            vec!["a", "b"],
            vec![Some(1), Some(2)],
            OffsetBuffer::<i32>::from_lengths(vec![2]),
            None,
        );

        let key_var = Expr::LambdaVariable(LambdaVariable::new(
            "k".to_string(),
            Some(Arc::new(Field::new("k", DataType::Utf8, false))),
        ));
        // (k, v) -> case when k = 'a' then v + 100 else v end
        let lambda_body = Expr::Case(datafusion_expr::expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(key_var.eq(lit("a"))),
                Box::new(value_var("v") + lit(100i32)),
            )],
            else_expr: Some(Box::new(value_var("v"))),
        });
        let result = eval_map_transform(map, lambda_body, ["k", "v"]).unwrap();
        let result_map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let result_values = result_map
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(result_values.values(), &[101, 2]);
    }

    #[test]
    fn map_transform_preserves_null_rows() {
        let map = create_str_int_map(
            vec!["a", "b", "c", "d"],
            vec![Some(1), Some(2), Some(3), Some(4)],
            OffsetBuffer::<i32>::from_lengths(vec![2, 2]),
            Some(NullBuffer::from(vec![true, false])),
        );

        let result =
            eval_map_transform(map, value_var("v") + lit(10i32), ["k", "v"]).unwrap();
        let result_map = result.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(result_map.is_valid(0));
        assert!(!result_map.is_valid(1));
        assert_eq!(result_map.value_length(0), 2);
    }
}
