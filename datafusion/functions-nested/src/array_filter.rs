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

//! [`ScalarUDFImpl`] definitions for array_filter function.

use arrow::array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, RecordBatch};
use arrow::buffer::OffsetBuffer;
use arrow::compute::filter;
use arrow::datatypes::DataType::{LargeList, List};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::cast::{as_boolean_array, as_large_list_array, as_list_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, plan_err, DFSchema, Result};
use datafusion_expr::expr::{schema_name_from_exprs_ref, ScalarFunction};
use datafusion_expr::{
    ColumnarValue, Documentation, ExprSchemable, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr::{Expr, LambdaPlanner, PhysicalLambda, ScalarUDF};
use datafusion_macros::user_doc;

use std::any::Any;
use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::sync::Arc;

use crate::utils::make_scalar_function;

make_udf_expr_and_func!(ArrayFilter,
    array_filter,
    array lambda, // arg names
    "filters array elements using a lambda function, returning a new array with elements where the lambda returns true.", // doc
    array_filter_udf // internal function name
);

/// Implementation of the `array_filter` scalar user-defined function.
/// 
/// This function filters array elements using a lambda function, returning a new array
/// containing only the elements for which the lambda function returns true.
/// 
/// The struct maintains both logical and physical representations of the lambda:
/// - `lambda`: The logical lambda expression from the SQL query
/// - `physical_lambda`: The planned physical lambda that can be executed
/// - `signature`: Function signature indicating it operates on arrays
#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Filters array elements using a lambda function.",
    syntax_example = "array_filter(array, lambda)",
    sql_example = r#"```sql
> select array_filter([1, 2, 3, 4, 5], x -> x > 3);
+--------------------------------------------------+
| array_filter(List([1,2,3,4,5]), x -> x > 3)      |
+--------------------------------------------------+
| [4, 5]                                           |
+--------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "lambda",
        description = "Lambda function with one argument that returns a boolean. The lambda is applied to each element of the array."
    )
)]
pub struct ArrayFilter {
    signature: Signature,
    lambda: Option<Box<Expr>>,
    physical_lambda: Option<Box<dyn PhysicalLambda>>,
}

impl std::fmt::Debug for ArrayFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayFilter")
            .field("signature", &self.signature)
            .field("lambda", &self.lambda)
            .field(
                "physical_lambda",
                if self.physical_lambda.is_some() {
                    &"<PhysicalLambda>"
                } else {
                    &"<None>"
                },
            )
            .finish()
    }
}

impl Default for ArrayFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrayFilter {
    /// Creates a new instance of ArrayFilter with default settings.
    /// 
    /// Initializes the function with an array signature and no lambda expressions.
    /// The lambda will be set later during query planning.
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            lambda: None,
            physical_lambda: None,
        }
    }

    /// Creates a new ArrayFilter instance with a physical lambda attached.
    /// 
    /// This is used during query execution when the logical lambda has been
    /// planned into an executable physical lambda.
    /// 
    /// # Arguments
    /// * `physical_lambda` - The planned physical lambda function
    fn with_physical_lambda(&self, physical_lambda: Box<dyn PhysicalLambda>) -> Self {
        Self {
            signature: self.signature.clone(),
            lambda: self.lambda.clone(),
            physical_lambda: Some(physical_lambda),
        }
    }

    /// Creates a new ArrayFilter instance with a logical lambda expression.
    /// 
    /// This is used during query planning when the lambda expression has been
    /// parsed but not yet converted to a physical representation.
    /// 
    /// # Arguments  
    /// * `lambda` - The logical lambda expression from the SQL query
    fn with_lambda(&self, lambda: &Expr) -> Self {
        Self {
            signature: self.signature.clone(),
            lambda: Some(Box::new(lambda.clone())),
            physical_lambda: None,
        }
    }
}

impl ScalarUDFImpl for ArrayFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_filter"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = take_function_args(self.name(), arg_types)?;
        match arg_type {
            List(_) | LargeList(_) => Ok(arg_type.clone()),
            _ => plan_err!("{} does not support type {}", self.name(), arg_type),
        }
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let Some(lambda) = self.physical_lambda.as_ref() else {
            if self.lambda.is_none() {
                return exec_err!("{} requires lambda", self.name());
            } else {
                return exec_err!("lambda in {} is not planned", self.name());
            }
        };
        make_scalar_function(|ar| -> Result<ArrayRef> {
            let [array] = take_function_args(self.name(), ar)?;
            array_filter_inner(array, lambda.as_ref())
        })(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn plan(
        &self,
        planner: &dyn LambdaPlanner,
        args: &[Expr],
        input_dfschema: &DFSchema,
    ) -> Result<Option<Arc<dyn ScalarUDFImpl>>> {
        let arg_types = args
            .iter()
            .map(|arg| arg.data_type_and_nullable(input_dfschema))
            .collect::<Result<Vec<_>>>()?;
        let arg_types = arg_types.iter().map(|(dt, _)| dt).collect::<Vec<_>>();
        match (self.lambda.as_ref(), arg_types.as_slice()) {
            (Some(lambda), &[List(field) | LargeList(field)]) => match lambda.as_ref() {
                Expr::Lambda(lambda) => {
                    let schema = Schema::new(vec![field
                        .as_ref()
                        .clone()
                        .with_name(lambda.params[0].clone())]);
                    let lambda_dfschema = DFSchema::try_from(schema)?;
                    let lambda_physical_lambda =
                        planner.plan_lambda(lambda, &lambda_dfschema)?;
                    Ok(Some(Arc::new(
                        self.with_physical_lambda(lambda_physical_lambda),
                    )))
                }
                _ => plan_err!("{} requires a lambda expression", self.name()),
            },
            _ => plan_err!(
                "{} requires List or LargeList as the first argument",
                self.name()
            ),
        }
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        let names: Vec<String> =
            self.args_with_lambda(args)?.iter().map(ToString::to_string).collect();
        Ok(std::format!("{}({})", self.name(), names.join(", ")))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let args = self.args_with_lambda(args)?;
        Ok(std::format!(
            "{}({})",
            self.name(),
            schema_name_from_exprs_ref(&args)?
        ))
    }

    fn try_call(&self, args: &[Expr]) -> Result<Option<Expr>> {
        match (self.lambda.as_ref(), args) {
            (Some(_), [_]) => Ok(None),
            (None, [array, lambda @ Expr::Lambda(func)]) => {
                if func.params.len() != 1 {
                    return exec_err!(
                        "{} requires a lambda with 1 argument",
                        self.name()
                    );
                }
                let func = Arc::new(ScalarUDF::new_from_impl(self.with_lambda(lambda)));
                let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
                    func,
                    vec![array.clone()],
                ));
                Ok(Some(expr))
            }
            _ => plan_err!("{} requires 1 argument and 1 lambda", self.name()),
        }
    }

    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {
        datafusion_common::not_impl_err!(
            "Function {} does not implement coerce_types",
            self.name()
        )
    }

    fn equals(&self, other: &dyn ScalarUDFImpl) -> bool {
        self.as_any().type_id() == other.as_any().type_id()
            && self.name() == other.name()
            && self.aliases() == other.aliases()
            && self.signature() == other.signature()
            && self.lambda.as_ref()
                == other
                    .as_any()
                    .downcast_ref::<Self>()
                    .unwrap()
                    .lambda
                    .as_ref()
    }

    fn hash_value(&self) -> u64 {
        let hasher = &mut DefaultHasher::new();
        self.as_any().type_id().hash(hasher);
        self.lambda.as_ref().hash(hasher);
        hasher.finish()
    }

    fn args_with_lambda<'a>(&'a self, args: &'a [Expr]) -> Result<Vec<&'a Expr>> {
        match (self.lambda.as_ref(), args) {
            (Some(lambda), [expr]) => Ok(vec![expr, lambda.as_ref()]),
            (None, [array, lambda]) if matches!(lambda, Expr::Lambda(_)) => {
                Ok(vec![array, lambda])
            }
            _ => plan_err!("{} requires 1 argument and 1 lambda", self.name()),
        }
    }
}

fn array_filter_inner(array: &ArrayRef, lambda: &dyn PhysicalLambda) -> Result<ArrayRef> {
    match array.data_type() {
        List(field) => {
            let array = as_list_array(&array)?;
            filter_generic_list_array(array, lambda, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(&array)?;
            filter_generic_list_array(array, lambda, field)
        }
        _ => exec_err!("array_filter does not support type {:?}", array.data_type()),
    }
}

fn filter_generic_list_array<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    lambda: &dyn PhysicalLambda,
    field: &Arc<Field>,
) -> Result<ArrayRef> {
    let mut offsets = vec![OffsetSize::zero()];

    let values = list_array.values();
    let value_offsets = list_array.value_offsets();
    let nulls = list_array.nulls();

    let batch = RecordBatch::try_new(
        Schema::new(vec![field
            .as_ref()
            .clone()
            .with_name(lambda.params()[0].clone())])
        .into(),
        vec![Arc::clone(values)],
    )?;

    let filter_array = lambda.evaluate(&batch)?;
    let ColumnarValue::Array(filter_array) = filter_array else {
        return exec_err!(
            "array_filter requires a lambda that returns an array of booleans"
        );
    };
    let filter_array = as_boolean_array(&filter_array)?;
    let filtered = filter(&values, filter_array)?;

    for row_index in 0..list_array.len() {
        if list_array.is_null(row_index) {
            // Handle null arrays by keeping the offset unchanged
            offsets.push(offsets[row_index]);
            continue;
        }
        let start = value_offsets[row_index];
        let end = value_offsets[row_index + 1];
        let num_true = filter_array
            .slice(start.as_usize(), (end - start).as_usize())
            .true_count();
        offsets.push(offsets[row_index] + OffsetSize::usize_as(num_true));
    }
    let offsets = OffsetBuffer::new(offsets.into());
    let list_array = GenericListArray::<OffsetSize>::try_new(
        Arc::clone(field),
        offsets,
        filtered,
        nulls.cloned(),
    )?;

    Ok(Arc::new(list_array))
}
