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

use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::{internal_err, Result};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use std::sync::Arc;

/// [`AccumulatorArgs`] contains information about how an aggregate
/// function was called, including the types of its arguments and any optional
/// ordering expressions.
#[derive(Debug, Clone)]
pub struct AccumulatorArgs<'a> {
    /// The return field of the aggregate function.
    pub return_field: FieldRef,

    /// The physical schema of the record batches fed into the aggregate.
    ///
    /// This is the same schema that [`exprs`] expect when resolving column
    /// references. For column-only aggregates the physical schema and the
    /// "effective" argument fields match one-to-one, so
    /// `acc_args.schema.field(i)` and
    /// `acc_args.exprs[i].return_field(&acc_args.schema)?` return equivalent
    /// metadata. For expressions that reference multiple columns (e.g.
    /// `SUM(a + b)`) the physical schema still contains the full input
    /// (`[a, b, …]`), while `return_field` synthesises the single argument
    /// field that the accumulator consumes. Both views are therefore exposed:
    ///
    /// * Use [`Self::schema`] to inspect the raw physical fields, including
    ///   metadata coming from the child plan.
    /// * Use [`Self::exprs`] in combination with [`PhysicalExpr::return_field`]
    ///   to recover the effective [`FieldRef`] for each aggregate argument.
    ///
    /// When an aggregate is invoked with only literal values, the physical
    /// schema is empty. In that case DataFusion synthesises a schema from the
    /// literal expressions so extension metadata is still available. In mixed
    /// column and literal inputs the existing physical schema takes precedence;
    /// synthesized metadata is only used when the physical schema is empty.
    ///
    /// For convenience, [`Self::input_field`] and [`Self::input_fields`] wrap
    /// the `exprs[i].return_field(&schema)` pattern so UDAF implementations can
    /// recover the effective [`FieldRef`]s without interacting with the raw
    /// schema directly, matching the ergonomics of other function argument
    /// structs.
    ///
    /// ### Relation to other function argument structs
    ///
    /// Scalar and window functions see arguments *after* their `PhysicalExpr`s
    /// have been evaluated. As a consequence, [`ScalarFunctionArgs`](datafusion_expr::ScalarFunctionArgs) and
    /// [`PartitionEvaluatorArgs`](datafusion_functions_window_common::partition::PartitionEvaluatorArgs)
    /// only expose the already-computed [`FieldRef`]s for each argument. In
    /// contrast, an accumulator is constructed before any batches have been
    /// processed, so the planner cannot simply hand the UDAF pre-evaluated
    /// arguments. Instead we provide both the physical schema and the original
    /// argument expressions; the accumulator can then call
    /// [`PhysicalExpr::return_field`] when it needs the logical argument field.
    ///
    /// This is why expressions such as `SUM(a + b)` require the schema even
    /// though scalar functions also support `SIN(a + b)`. The scalar version
    /// receives the evaluated `a + b` as a [`ColumnarValue`](datafusion_expr::ColumnarValue), while the
    /// aggregate still holds the unevaluated `PhysicalExpr` and must resolve it
    /// against the physical schema when computing its metadata.
    pub schema: &'a Schema,

    /// Whether to ignore nulls.
    ///
    /// SQL allows the user to specify `IGNORE NULLS`, for example:
    ///
    /// ```sql
    /// SELECT FIRST_VALUE(column1) IGNORE NULLS FROM t;
    /// ```
    pub ignore_nulls: bool,

    /// The expressions in the `ORDER BY` clause passed to this aggregator.
    ///
    /// SQL allows the user to specify the ordering of arguments to the
    /// aggregate using an `ORDER BY`. For example:
    ///
    /// ```sql
    /// SELECT FIRST_VALUE(column1 ORDER BY column2) FROM t;
    /// ```
    pub order_bys: &'a [PhysicalSortExpr],

    /// Whether the aggregation is running in reverse order
    pub is_reversed: bool,

    /// The name of the aggregate expression
    pub name: &'a str,

    /// Whether the aggregate function is distinct.
    ///
    /// ```sql
    /// SELECT COUNT(DISTINCT column1) FROM t;
    /// ```
    pub is_distinct: bool,

    /// The physical expressions for the aggregate function's arguments.
    /// Use these expressions together with [`Self::schema`] to obtain the
    /// [`FieldRef`] of each input via
    /// [`PhysicalExpr::return_field`](`PhysicalExpr::return_field`).
    ///
    /// Example:
    /// ```ignore
    /// let input_field = exprs[i].return_field(&schema)?;
    /// ```
    /// Note: physical schema metadata takes precedence in mixed inputs.
    pub exprs: &'a [Arc<dyn PhysicalExpr>],
}

impl AccumulatorArgs<'_> {
    /// Returns the return type of the aggregate function.
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }

    /// Returns the [`FieldRef`] corresponding to the `index`th aggregate
    /// argument.
    pub fn input_field(&self, index: usize) -> Result<FieldRef> {
        let expr = self.exprs.get(index).ok_or_else(|| {
            internal_err!(
                "input_field index {index} is out of bounds for {} arguments",
                self.exprs.len()
            )
        })?;

        expr.return_field(self.schema)
    }

    /// Returns [`FieldRef`]s for all aggregate arguments in order.
    pub fn input_fields(&self) -> Result<Vec<FieldRef>> {
        self.exprs
            .iter()
            .map(|expr| expr.return_field(self.schema))
            .collect()
    }
}

/// Factory that returns an accumulator for the given aggregate function.
pub type AccumulatorFactoryFunction =
    Arc<dyn Fn(AccumulatorArgs) -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// [`StateFieldsArgs`] contains information about the fields that an
/// aggregate function's accumulator should have. Used for `AggregateUDFImpl::state_fields`.
pub struct StateFieldsArgs<'a> {
    /// The name of the aggregate function.
    pub name: &'a str,

    /// The input fields of the aggregate function.
    pub input_fields: &'a [FieldRef],

    /// The return fields of the aggregate function.
    pub return_field: FieldRef,

    /// The ordering fields of the aggregate function.
    pub ordering_fields: &'a [FieldRef],

    /// Whether the aggregate function is distinct.
    pub is_distinct: bool,
}

impl StateFieldsArgs<'_> {
    /// The return type of the aggregate function.
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }
}
