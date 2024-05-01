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

//! [`AggregateUDF`]: User Defined Aggregate Functions

use std::fmt::Debug;
use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{not_impl_err, Result};
use datafusion_expr_common::groups_accumulator::GroupsAccumulator;
use datafusion_expr_common::{accumulator::Accumulator, signature::Signature};

/// [`AccumulatorArgs`] contains information about how an aggregate
/// function was called, including the types of its arguments and any optional
/// ordering expressions.
pub struct AccumulatorArgs<'a> {
    /// The return type of the aggregate function.
    pub data_type: &'a DataType,
    /// The schema of the input arguments
    pub schema: &'a Schema,
    /// Whether to ignore nulls.
    ///
    /// SQL allows the user to specify `IGNORE NULLS`, for example:
    ///
    /// ```sql
    /// SELECT FIRST_VALUE(column1) IGNORE NULLS FROM t;
    /// ```
    pub ignore_nulls: bool,
    // / The expressions in the `ORDER BY` clause passed to this aggregator.
    // /
    // / SQL allows the user to specify the ordering of arguments to the
    // / aggregate using an `ORDER BY`. For example:
    // /
    // / ```sql
    // / SELECT FIRST_VALUE(column1 ORDER BY column2) FROM t;
    // / ```
    // /
    // / If no `ORDER BY` is specified, `sort_exprs`` will be empty.
    // pub ordering_req: &'a LexOrdering,
}

impl<'a> AccumulatorArgs<'a> {
    pub fn new(data_type: &'a DataType, schema: &'a Schema, ignore_nulls: bool) -> Self {
        Self {
            data_type,
            schema,
            ignore_nulls,
        }
    }
}

/// Factory that returns an accumulator for the given aggregate function.
pub type AccumulatorFactoryFunction =
    Arc<dyn Fn(AccumulatorArgs) -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// Trait for implementing [`AggregateUDF`].
///
/// This trait exposes the full API for implementing user defined aggregate functions and
/// can be used to implement any function.
///
/// See [`advanced_udaf.rs`] for a full example with complete implementation and
/// [`AggregateUDF`] for other available options.
///
/// [`advanced_udaf.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_udaf.rs
///
/// # Basic Example
/// ```
/// # use std::any::Any;
/// # use arrow::datatypes::DataType;
/// # use datafusion_common::{DataFusionError, plan_err, Result};
/// # use datafusion_expr::{col, ColumnarValue, Signature, Volatility, Expr};
/// # use datafusion_expr::{AggregateUDFImpl, AggregateUDF, Accumulator, function::AccumulatorArgs};
/// # use arrow::datatypes::Schema;
/// # use arrow::datatypes::Field;
/// #[derive(Debug, Clone)]
/// struct GeoMeanUdf {
///   signature: Signature
/// };
///
/// impl GeoMeanUdf {
///   fn new() -> Self {
///     Self {
///       signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable)
///      }
///   }
/// }
///
/// /// Implement the AggregateUDFImpl trait for GeoMeanUdf
/// impl AggregateUDFImpl for GeoMeanUdf {
///    fn as_any(&self) -> &dyn Any { self }
///    fn name(&self) -> &str { "geo_mean" }
///    fn signature(&self) -> &Signature { &self.signature }
///    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
///      if !matches!(args.get(0), Some(&DataType::Float64)) {
///        return plan_err!("add_one only accepts Float64 arguments");
///      }
///      Ok(DataType::Float64)
///    }
///    // This is the accumulator factory; DataFusion uses it to create new accumulators.
///    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> { unimplemented!() }
///    fn state_fields(&self, _name: &str, value_type: DataType, _ordering_fields: Vec<Field>) -> Result<Vec<Field>> {
///        Ok(vec![
///             Field::new("value", value_type, true),
///             Field::new("ordering", DataType::UInt32, true)
///        ])
///    }
/// }
///
/// // Create a new AggregateUDF from the implementation
/// let geometric_mean = AggregateUDF::from(GeoMeanUdf::new());
///
/// // Call the function `geo_mean(col)`
/// let expr = geometric_mean.call(vec![col("a")]);
/// ```
pub trait AggregateUDFImpl: Debug + Send + Sync {
    /// Returns this object as an [`Any`] trait object
    fn as_any(&self) -> &dyn Any;

    /// Returns this function's name
    fn name(&self) -> &str;

    /// Returns the function's [`Signature`] for information about what input
    /// types are accepted and the function's Volatility.
    fn signature(&self) -> &Signature;

    /// What [`DataType`] will be returned by this function, given the types of
    /// the arguments
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// Return a new [`Accumulator`] that aggregates values for a specific
    /// group during query execution.
    ///
    /// acc_args: [`AccumulatorArgs`] contains information about how the
    /// aggregate function was called.
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>>;

    /// Return the fields used to store the intermediate state of this accumulator.
    ///
    /// # Arguments:
    /// 1. `name`: the name of the expression (e.g. AVG, SUM, etc)
    /// 2. `value_type`: Aggregate's aggregate's output (returned by [`Self::return_type`])
    /// 3. `ordering_fields`: the fields used to order the input arguments, if any.
    ///     Empty if no ordering expression is provided.
    ///
    /// # Notes:
    ///
    /// The default implementation returns a single state field named `name`
    /// with the same type as `value_type`. This is suitable for aggregates such
    /// as `SUM` or `MIN` where partial state can be combined by applying the
    /// same aggregate.
    ///
    /// For aggregates such as `AVG` where the partial state is more complex
    /// (e.g. a COUNT and a SUM), this method is used to define the additional
    /// fields.
    ///
    /// The name of the fields must be unique within the query and thus should
    /// be derived from `name`. See [`format_state_name`] for a utility function
    /// to generate a unique name.
    fn state_fields(
        &self,
        name: &str,
        value_type: DataType,
        ordering_fields: Vec<Field>,
    ) -> Result<Vec<Field>> {
        let value_fields = vec![Field::new(
            format_state_name(name, "value"),
            value_type,
            true,
        )];

        Ok(value_fields.into_iter().chain(ordering_fields).collect())
    }

    /// If the aggregate expression has a specialized
    /// [`GroupsAccumulator`] implementation. If this returns true,
    /// `[Self::create_groups_accumulator]` will be called.
    ///
    /// # Notes
    ///
    /// Even if this function returns true, DataFusion will still use
    /// `Self::accumulator` for certain queries, such as when this aggregate is
    /// used as a window function or when there no GROUP BY columns in the
    /// query.
    fn groups_accumulator_supported(&self) -> bool {
        false
    }

    /// Return a specialized [`GroupsAccumulator`] that manages state
    /// for all groups.
    ///
    /// For maximum performance, a [`GroupsAccumulator`] should be
    /// implemented in addition to [`Accumulator`].
    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("GroupsAccumulator hasn't been implemented for {self:?} yet")
    }

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &[]
    }
}

/// Build state name. State is the intermidiate state of the aggregate function.
pub fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{name}[{state_name}]")
}
