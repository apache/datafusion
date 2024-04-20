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

use std::any::Any;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::DataPtr;
use datafusion_common::{internal_err, not_impl_err, DFSchema, Result};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::Alias;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::{ColumnarValue, Expr};

use crate::expressions::column::Column;
use crate::expressions::literal::Literal;
use crate::sort_properties::SortProperties;
use crate::utils::scatter;

/// See [create_physical_expr](https://docs.rs/datafusion/latest/datafusion/physical_expr/fn.create_physical_expr.html)
/// for examples of creating `PhysicalExpr` from `Expr`
pub trait PhysicalExpr: Send + Sync + Display + Debug + PartialEq<dyn Any> {
    /// Returns the physical expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType>;
    /// Determine whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool>;
    /// Evaluate an expression against a RecordBatch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue>;
    /// Evaluate an expression against a RecordBatch after first applying a
    /// validity array
    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        let tmp_batch = filter_record_batch(batch, selection)?;

        let tmp_result = self.evaluate(&tmp_batch)?;

        if batch.num_rows() == tmp_batch.num_rows() {
            // All values from the `selection` filter are true.
            Ok(tmp_result)
        } else if let ColumnarValue::Array(a) = tmp_result {
            scatter(selection, a.as_ref()).map(ColumnarValue::Array)
        } else {
            Ok(tmp_result)
        }
    }

    /// Get a list of child PhysicalExpr that provide the input for this expr.
    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Returns a new PhysicalExpr where all children were replaced by new exprs.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// Computes the output interval for the expression, given the input
    /// intervals.
    ///
    /// # Arguments
    ///
    /// * `children` are the intervals for the children (inputs) of this
    /// expression.
    ///
    /// # Example
    ///
    /// If the expression is `a + b`, and the input intervals are `a: [1, 2]`
    /// and `b: [3, 4]`, then the output interval would be `[4, 6]`.
    fn evaluate_bounds(&self, _children: &[&Interval]) -> Result<Interval> {
        not_impl_err!("Not implemented for {self}")
    }

    /// Updates bounds for child expressions, given a known interval for this
    /// expression.
    ///
    /// This is used to propagate constraints down through an expression tree.
    ///
    /// # Arguments
    ///
    /// * `interval` is the currently known interval for this expression.
    /// * `children` are the current intervals for the children of this expression.
    ///
    /// # Returns
    ///
    /// A `Vec` of new intervals for the children, in order.
    ///
    /// If constraint propagation reveals an infeasibility for any child, returns
    /// [`None`]. If none of the children intervals change as a result of propagation,
    /// may return an empty vector instead of cloning `children`. This is the default
    /// (and conservative) return value.
    ///
    /// # Example
    ///
    /// If the expression is `a + b`, the current `interval` is `[4, 5]` and the
    /// inputs `a` and `b` are respectively given as `[0, 2]` and `[-∞, 4]`, then
    /// propagation would would return `[0, 2]` and `[2, 4]` as `b` must be at
    /// least `2` to make the output at least `4`.
    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        Ok(Some(vec![]))
    }

    /// Update the hash `state` with this expression requirements from
    /// [`Hash`].
    ///
    /// This method is required to support hashing [`PhysicalExpr`]s.  To
    /// implement it, typically the type implementing
    /// [`PhysicalExpr`] implements [`Hash`] and
    /// then the following boiler plate is used:
    ///
    /// # Example:
    /// ```
    /// // User defined expression that derives Hash
    /// #[derive(Hash, Debug, PartialEq, Eq)]
    /// struct MyExpr {
    ///   val: u64
    /// }
    ///
    /// // impl PhysicalExpr {
    /// // ...
    /// # impl MyExpr {
    ///   // Boiler plate to call the derived Hash impl
    ///   fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
    ///     use std::hash::Hash;
    ///     let mut s = state;
    ///     self.hash(&mut s);
    ///   }
    /// // }
    /// # }
    /// ```
    /// Note: [`PhysicalExpr`] is not constrained by [`Hash`]
    /// directly because it must remain object safe.
    fn dyn_hash(&self, _state: &mut dyn Hasher);

    /// The order information of a PhysicalExpr can be estimated from its children.
    /// This is especially helpful for projection expressions. If we can ensure that the
    /// order of a PhysicalExpr to project matches with the order of SortExec, we can
    /// eliminate that SortExecs.
    ///
    /// By recursively calling this function, we can obtain the overall order
    /// information of the PhysicalExpr. Since `SortOptions` cannot fully handle
    /// the propagation of unordered columns and literals, the `SortProperties`
    /// struct is used.
    fn get_ordering(&self, _children: &[SortProperties]) -> SortProperties {
        SortProperties::Unordered
    }
}

impl Hash for dyn PhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

/// Returns a copy of this expr if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `PhysicalExpr::children()`.
pub fn with_new_children_if_necessary(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let old_children = expr.children();
    if children.len() != old_children.len() {
        internal_err!("PhysicalExpr: Wrong number of children")
    } else if children.is_empty()
        || children
            .iter()
            .zip(old_children.iter())
            .any(|(c1, c2)| !Arc::data_ptr_eq(c1, c2))
    {
        Ok(expr.with_new_children(children)?)
    } else {
        Ok(expr)
    }
}

pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn PhysicalExpr>>() {
        any.downcast_ref::<Arc<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn PhysicalExpr>>() {
        any.downcast_ref::<Box<dyn PhysicalExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

/// [PhysicalExpr] evaluate DataFusion expressions such as `A + 1`, or `CAST(c1
/// AS int)`.
///
/// [PhysicalExpr] are the physical counterpart to [Expr] used in logical
/// planning, and can be evaluated directly on a [RecordBatch]. They are
/// normally created from [Expr] by a [PhysicalPlanner] and can be created
/// directly using [create_physical_expr].
///
/// A Physical expression knows its type, nullability and how to evaluate itself.
///
/// [PhysicalPlanner]: https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.PhysicalPlanner.html
/// [RecordBatch]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
///
/// # Example: Create `PhysicalExpr` from `Expr`
/// ```
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::DFSchema;
/// # use datafusion_expr::{Expr, col, lit};
/// # use datafusion_physical_expr::create_physical_expr;
/// # use datafusion_expr::execution_props::ExecutionProps;
/// // For a logical expression `a = 1`, we can create a physical expression
/// let expr = col("a").eq(lit(1));
/// // To create a PhysicalExpr we need 1. a schema
/// let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
/// let df_schema = DFSchema::try_from(schema).unwrap();
/// // 2. ExecutionProps
/// let props = ExecutionProps::new();
/// // We can now create a PhysicalExpr:
/// let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
/// ```
///
/// # Example: Executing a PhysicalExpr to obtain [ColumnarValue]
/// ```
/// # use std::sync::Arc;
/// # use arrow::array::{cast::AsArray, BooleanArray, Int32Array, RecordBatch};
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::{assert_batches_eq, DFSchema};
/// # use datafusion_expr::{Expr, col, lit, ColumnarValue};
/// # use datafusion_physical_expr::create_physical_expr;
/// # use datafusion_expr::execution_props::ExecutionProps;
/// # let expr = col("a").eq(lit(1));
/// # let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
/// # let df_schema = DFSchema::try_from(schema.clone()).unwrap();
/// # let props = ExecutionProps::new();
/// // Given a PhysicalExpr, for `a = 1` we can evaluate it against a RecordBatch like this:
/// let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
/// // Input of [1,2,3]
/// let input_batch = RecordBatch::try_from_iter(vec![
///   ("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as _)
/// ]).unwrap();
/// // The result is a ColumnarValue (either an Array or a Scalar)
/// let result = physical_expr.evaluate(&input_batch).unwrap();
/// // In this case, a BooleanArray with the result of the comparison
/// let ColumnarValue::Array(arr) = result else {
///  panic!("Expected an array")
/// };
/// assert_eq!(arr.as_boolean(), &BooleanArray::from(vec![true, false, false]));
/// ```
///
/// [ColumnarValue]: datafusion_expr::ColumnarValue
///
/// Create a physical expression from a logical expression ([Expr]).
///
/// # Arguments
///
/// * `e` - The logical expression
/// * `input_dfschema` - The DataFusion schema for the input, used to resolve `Column` references
///                      to qualified or unqualified fields by name.
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let _input_schema: &Schema = &input_dfschema.into();

    match e {
        Expr::Alias(Alias { expr, .. }) => {
            Ok(create_physical_expr(expr, input_dfschema, execution_props)?)
        }
        Expr::Column(c) => {
            let idx = input_dfschema.index_of_column(c)?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        // Expr::ScalarVariable(_, variable_names) => {
        //     if is_system_variables(variable_names) {
        //         match execution_props.get_var_provider(VarType::System) {
        //             Some(provider) => {
        //                 let scalar_value = provider.get_value(variable_names.clone())?;
        //                 Ok(Arc::new(Literal::new(scalar_value)))
        //             }
        //             _ => plan_err!("No system variable provider found"),
        //         }
        //     } else {
        //         match execution_props.get_var_provider(VarType::UserDefined) {
        //             Some(provider) => {
        //                 let scalar_value = provider.get_value(variable_names.clone())?;
        //                 Ok(Arc::new(Literal::new(scalar_value)))
        //             }
        //             _ => plan_err!("No user defined variable provider found"),
        //         }
        //     }
        // }
        // Expr::IsTrue(expr) => {
        //     let binary_op = binary_expr(
        //         expr.as_ref().clone(),
        //         Operator::IsNotDistinctFrom,
        //         Expr::Literal(ScalarValue::Boolean(Some(true))),
        //     );
        //     create_physical_expr(&binary_op, input_dfschema, execution_props)
        // }
        // Expr::IsNotTrue(expr) => {
        //     let binary_op = binary_expr(
        //         expr.as_ref().clone(),
        //         Operator::IsDistinctFrom,
        //         Expr::Literal(ScalarValue::Boolean(Some(true))),
        //     );
        //     create_physical_expr(&binary_op, input_dfschema, execution_props)
        // }
        // Expr::IsFalse(expr) => {
        //     let binary_op = binary_expr(
        //         expr.as_ref().clone(),
        //         Operator::IsNotDistinctFrom,
        //         Expr::Literal(ScalarValue::Boolean(Some(false))),
        //     );
        //     create_physical_expr(&binary_op, input_dfschema, execution_props)
        // }
        // Expr::IsNotFalse(expr) => {
        //     let binary_op = binary_expr(
        //         expr.as_ref().clone(),
        //         Operator::IsDistinctFrom,
        //         Expr::Literal(ScalarValue::Boolean(Some(false))),
        //     );
        //     create_physical_expr(&binary_op, input_dfschema, execution_props)
        // }
        // Expr::IsUnknown(expr) => {
        //     let binary_op = binary_expr(
        //         expr.as_ref().clone(),
        //         Operator::IsNotDistinctFrom,
        //         Expr::Literal(ScalarValue::Boolean(None)),
        //     );
        //     create_physical_expr(&binary_op, input_dfschema, execution_props)
        // }
        // Expr::IsNotUnknown(expr) => {
        //     let binary_op = binary_expr(
        //         expr.as_ref().clone(),
        //         Operator::IsDistinctFrom,
        //         Expr::Literal(ScalarValue::Boolean(None)),
        //     );
        //     create_physical_expr(&binary_op, input_dfschema, execution_props)
        // }
        // Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
        //     // Create physical expressions for left and right operands
        //     let lhs = create_physical_expr(left, input_dfschema, execution_props)?;
        //     let rhs = create_physical_expr(right, input_dfschema, execution_props)?;
        //     // Note that the logical planner is responsible
        //     // for type coercion on the arguments (e.g. if one
        //     // argument was originally Int32 and one was
        //     // Int64 they will both be coerced to Int64).
        //     //
        //     // There should be no coercion during physical
        //     // planning.
        //     binary(lhs, *op, rhs, input_schema)
        // }
        // Expr::Like(Like {
        //     negated,
        //     expr,
        //     pattern,
        //     escape_char,
        //     case_insensitive,
        // }) => {
        //     if escape_char.is_some() {
        //         return exec_err!("LIKE does not support escape_char");
        //     }
        //     let physical_expr =
        //         create_physical_expr(expr, input_dfschema, execution_props)?;
        //     let physical_pattern =
        //         create_physical_expr(pattern, input_dfschema, execution_props)?;
        //     like(
        //         *negated,
        //         *case_insensitive,
        //         physical_expr,
        //         physical_pattern,
        //         input_schema,
        //     )
        // }
        // Expr::Case(case) => {
        //     let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = &case.expr {
        //         Some(create_physical_expr(
        //             e.as_ref(),
        //             input_dfschema,
        //             execution_props,
        //         )?)
        //     } else {
        //         None
        //     };
        //     let (when_expr, then_expr): (Vec<&Expr>, Vec<&Expr>) = case
        //         .when_then_expr
        //         .iter()
        //         .map(|(w, t)| (w.as_ref(), t.as_ref()))
        //         .unzip();
        //     let when_expr =
        //         create_physical_exprs(when_expr, input_dfschema, execution_props)?;
        //     let then_expr =
        //         create_physical_exprs(then_expr, input_dfschema, execution_props)?;
        //     let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
        //         when_expr
        //             .iter()
        //             .zip(then_expr.iter())
        //             .map(|(w, t)| (w.clone(), t.clone()))
        //             .collect();
        //     let else_expr: Option<Arc<dyn PhysicalExpr>> =
        //         if let Some(e) = &case.else_expr {
        //             Some(create_physical_expr(
        //                 e.as_ref(),
        //                 input_dfschema,
        //                 execution_props,
        //             )?)
        //         } else {
        //             None
        //         };
        //     Ok(expressions::case(expr, when_then_expr, else_expr)?)
        // }
        // Expr::Cast(Cast { expr, data_type }) => expressions::cast(
        //     create_physical_expr(expr, input_dfschema, execution_props)?,
        //     input_schema,
        //     data_type.clone(),
        // ),
        // Expr::TryCast(TryCast { expr, data_type }) => expressions::try_cast(
        //     create_physical_expr(expr, input_dfschema, execution_props)?,
        //     input_schema,
        //     data_type.clone(),
        // ),
        // Expr::Not(expr) => {
        //     expressions::not(create_physical_expr(expr, input_dfschema, execution_props)?)
        // }
        // Expr::Negative(expr) => expressions::negative(
        //     create_physical_expr(expr, input_dfschema, execution_props)?,
        //     input_schema,
        // ),
        // Expr::IsNull(expr) => expressions::is_null(create_physical_expr(
        //     expr,
        //     input_dfschema,
        //     execution_props,
        // )?),
        // Expr::IsNotNull(expr) => expressions::is_not_null(create_physical_expr(
        //     expr,
        //     input_dfschema,
        //     execution_props,
        // )?),
        // Expr::GetIndexedField(GetIndexedField { expr: _, field }) => match field {
        //     GetFieldAccess::NamedStructField { name: _ } => {
        //         internal_err!(
        //             "NamedStructField should be rewritten in OperatorToFunction"
        //         )
        //     }
        //     GetFieldAccess::ListIndex { key: _ } => {
        //         internal_err!("ListIndex should be rewritten in OperatorToFunction")
        //     }
        //     GetFieldAccess::ListRange {
        //         start: _,
        //         stop: _,
        //         stride: _,
        //     } => {
        //         internal_err!("ListRange should be rewritten in OperatorToFunction")
        //     }
        // },

        // Expr::ScalarFunction(ScalarFunction { func_def, args }) => {
        //     let physical_args =
        //         create_physical_exprs(args, input_dfschema, execution_props)?;

        //     match func_def {
        //         ScalarFunctionDefinition::BuiltIn(fun) => {
        //             functions::create_builtin_physical_expr(
        //                 fun,
        //                 &physical_args,
        //                 input_schema,
        //                 execution_props,
        //             )
        //         }
        //         ScalarFunctionDefinition::UDF(fun) => udf::create_physical_expr(
        //             fun.clone().as_ref(),
        //             &physical_args,
        //             input_schema,
        //             args,
        //             input_dfschema,
        //         ),
        //         ScalarFunctionDefinition::Name(_) => {
        //             internal_err!("Function `Expr` with name should be resolved.")
        //         }
        //     }
        // }
        // Expr::Between(Between {
        //     expr,
        //     negated,
        //     low,
        //     high,
        // }) => {
        //     let value_expr = create_physical_expr(expr, input_dfschema, execution_props)?;
        //     let low_expr = create_physical_expr(low, input_dfschema, execution_props)?;
        //     let high_expr = create_physical_expr(high, input_dfschema, execution_props)?;

        //     // rewrite the between into the two binary operators
        //     let binary_expr = binary(
        //         binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
        //         Operator::And,
        //         binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
        //         input_schema,
        //     );

        //     if *negated {
        //         expressions::not(binary_expr?)
        //     } else {
        //         binary_expr
        //     }
        // }
        // Expr::InList(InList {
        //     expr,
        //     list,
        //     negated,
        // }) => match expr.as_ref() {
        //     Expr::Literal(ScalarValue::Utf8(None)) => {
        //         Ok(expressions::lit(ScalarValue::Boolean(None)))
        //     }
        //     _ => {
        //         let value_expr =
        //             create_physical_expr(expr, input_dfschema, execution_props)?;

        //         let list_exprs =
        //             create_physical_exprs(list, input_dfschema, execution_props)?;
        //         expressions::in_list(value_expr, list_exprs, negated, input_schema)
        //     }
        // },
        other => {
            not_impl_err!("Physical plan does not support logical expression {other:?}")
        }
    }
}

/// Create vector of Physical Expression from a vector of logical expression
pub fn create_physical_exprs<'a, I>(
    exprs: I,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Vec<Arc<dyn PhysicalExpr>>>
where
    I: IntoIterator<Item = &'a Expr>,
{
    exprs
        .into_iter()
        .map(|expr| create_physical_expr(expr, input_dfschema, execution_props))
        .collect::<Result<Vec<_>>>()
}