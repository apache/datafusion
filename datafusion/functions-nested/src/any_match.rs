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

//! [`HigherOrderUDF`] definitions for array_any_match function.

use arrow::{
    array::{Array, AsArray, BooleanArray, new_null_array},
    datatypes::{ArrowNativeType, DataType, Field, FieldRef},
};
use datafusion_common::{
    Result, exec_err, plan_err,
    utils::{list_values, take_function_args},
};
use datafusion_expr::{
    ColumnarValue, Documentation, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDF, LambdaParametersProgress, ValueOrLambda,
    Volatility,
};
use datafusion_macros::user_doc;
use std::{fmt::Debug, sync::Arc};

make_higher_order_function_expr_and_func!(
    ArrayAnyMatch,
    array_any_match,
    array lambda,
    "returns true if any element in the array satisfies the predicate",
    array_any_match_higher_order_function
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns whether any elements of an array match the given predicate. Returns true if one or more elements match, false if none match (including empty arrays), and null if the predicate returns null for some elements and false for all others.",
    syntax_example = "any_match(array, predicate)",
    sql_example = r#"```sql
> select any_match([1, 2, 3], x -> x > 2);
+----------------------------------+
| any_match([1, 2, 3], x -> x > 2) |
+----------------------------------+
| true                             |
+----------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "predicate",
        description = "Lambda predicate that returns a boolean"
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayAnyMatch {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl Default for ArrayAnyMatch {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayAnyMatch {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("any_match"), String::from("list_any_match")],
        }
    }
}

// Returns Some(true) if any element in [start, end) is true,
// None if no element is true but some are null,
// Some(false) if all are false or range is empty.
fn any_match_for_range(
    predicate: &BooleanArray,
    start: usize,
    end: usize,
) -> Option<bool> {
    let any_true = (start..end).any(|j| predicate.is_valid(j) && predicate.value(j));
    if any_true {
        return Some(true);
    }
    let any_null = (start..end).any(|j| predicate.is_null(j));
    if any_null { None } else { Some(false) }
}

impl HigherOrderUDF for ArrayAnyMatch {
    fn name(&self) -> &str {
        "array_any_match"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let list = if arg_types.len() == 1 {
            &arg_types[0]
        } else {
            return plan_err!(
                "{} function requires 1 value argument, got {}",
                self.name(),
                arg_types.len()
            );
        };

        let coerced = match list {
            DataType::List(_) | DataType::LargeList(_) => list.clone(),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                DataType::List(Arc::clone(field))
            }
            DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
            _ => {
                return plan_err!(
                    "{} expected a list as first argument, got {}",
                    self.name(),
                    list
                );
            }
        };

        Ok(vec![coerced])
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let [list, _lambda] = take_function_args(self.name(), fields)?;

        let field = match list {
            ValueOrLambda::Value(f) => match f.data_type() {
                DataType::List(field) => field,
                DataType::LargeList(field) => field,
                other => return plan_err!("expected list, got {other}"),
            },
            _ => return plan_err!("{} expected a value as first argument", self.name()),
        };

        Ok(LambdaParametersProgress::Complete(vec![vec![Arc::clone(
            field,
        )]]))
    }

    fn return_field_from_args(
        &self,
        args: HigherOrderReturnFieldArgs,
    ) -> Result<Arc<Field>> {
        let [list, _lambda] = take_function_args(self.name(), args.arg_fields)?;
        let nullable = matches!(list, ValueOrLambda::Value(f) if f.is_nullable());
        Ok(Arc::new(Field::new("", DataType::Boolean, nullable)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let [list, lambda] = take_function_args(self.name(), &args.args)?;

        let (ValueOrLambda::Value(list), ValueOrLambda::Lambda(lambda)) = (list, lambda)
        else {
            return exec_err!("{} expects a value followed by a lambda", self.name());
        };

        let list_array = list.to_array(args.number_rows)?;

        // fast path: fully null input — also required for FixedSizeList which can't be
        // handled by clear_null_values when fully null
        if list_array.null_count() == list_array.len() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                list_array.len(),
            )));
        }

        let list_values = list_values(&list_array)?;

        let values_param = || Ok(Arc::clone(&list_values));

        let predicate_results = lambda
            .evaluate(&[&values_param])?
            .into_array(list_values.len())?;

        let predicate_bool = predicate_results
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(format!(
                    "{} predicate must return boolean array",
                    self.name()
                ))
            })?;

        let mut results = Vec::with_capacity(list_array.len());

        macro_rules! process_list {
            ($list_typed:expr) => {{
                let offsets = $list_typed.offsets();
                for i in 0..$list_typed.len() {
                    if $list_typed.is_null(i) {
                        results.push(None);
                        continue;
                    }
                    let start = offsets[i].as_usize();
                    let end = offsets[i + 1].as_usize();
                    results.push(any_match_for_range(predicate_bool, start, end));
                }
            }};
        }

        match list_array.data_type() {
            DataType::List(_) => {
                process_list!(list_array.as_list::<i32>());
            }
            DataType::LargeList(_) => {
                process_list!(list_array.as_list::<i64>());
            }
            other => return exec_err!("expected list, got {other}"),
        }

        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(results))))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    };
    use datafusion_common::{DFSchema, Result};
    use datafusion_expr::{
        Expr, col,
        execution_props::ExecutionProps,
        expr::{HigherOrderFunction, LambdaVariable},
        lambda, lit,
    };
    use datafusion_physical_expr::create_physical_expr;

    use crate::any_match::array_any_match_higher_order_function;

    fn run_any_match(
        list: impl arrow::array::Array + Clone + 'static,
    ) -> Result<ArrayRef> {
        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new(
                "list",
                list.data_type().clone(),
                list.is_nullable(),
            )]
            .into(),
            HashMap::new(),
        )?;

        create_physical_expr(
            &Expr::HigherOrderFunction(HigherOrderFunction::new(
                array_any_match_higher_order_function(),
                vec![
                    col("list"),
                    lambda(
                        ["x"],
                        Expr::LambdaVariable(LambdaVariable::new(
                            "x".to_string(),
                            Some(Arc::new(Field::new("x", DataType::Int32, true))),
                        ))
                        .gt(lit(2i32)),
                    ),
                ],
            )),
            &schema,
            &ExecutionProps::new(),
        )?
        .evaluate(&RecordBatch::try_new(
            Arc::clone(schema.inner()),
            vec![Arc::new(list.clone())],
        )?)?
        .into_array(list.len())
    }

    fn make_list(values: Vec<i32>, offsets: OffsetBuffer<i32>) -> ListArray {
        ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            offsets,
            Arc::new(Int32Array::from(values)),
            None,
        )
    }

    #[test]
    fn test_any_match_some_true() -> Result<()> {
        let list = make_list(vec![1, 2, 3], OffsetBuffer::from_lengths(vec![3]));
        let result = run_any_match(list)?;
        assert_eq!(
            result.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(true)])
        );
        Ok(())
    }

    #[test]
    fn test_any_match_none_true() -> Result<()> {
        let list = make_list(vec![1, 2], OffsetBuffer::from_lengths(vec![2]));
        let result = run_any_match(list)?;
        assert_eq!(
            result.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(false)])
        );
        Ok(())
    }

    #[test]
    fn test_any_match_empty_array() -> Result<()> {
        let list = make_list(vec![], OffsetBuffer::from_lengths(vec![0]));
        let result = run_any_match(list)?;
        assert_eq!(
            result.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(false)])
        );
        Ok(())
    }

    #[test]
    fn test_any_match_multiple_rows() -> Result<()> {
        let list = make_list(vec![1, 2, 3, 1, 2], OffsetBuffer::from_lengths(vec![3, 2]));
        let result = run_any_match(list)?;
        assert_eq!(
            result.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(true), Some(false)])
        );
        Ok(())
    }
}
