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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::{
    physical_expr::PhysicalExpr, sort_expr::PhysicalSortExpr,
};
use std::sync::Arc;

/// [`AccumulatorArgs`] contains information about how an aggregate
/// function was called, including the types of its arguments and any optional
/// ordering expressions.
#[derive(Debug)]
pub struct AccumulatorArgs<'a> {
    /// The return type of the aggregate function.
    pub return_type: &'a DataType,

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

    /// The expressions in the `ORDER BY` clause passed to this aggregator.
    ///
    /// SQL allows the user to specify the ordering of arguments to the
    /// aggregate using an `ORDER BY`. For example:
    ///
    /// ```sql
    /// SELECT FIRST_VALUE(column1 ORDER BY column2) FROM t;
    /// ```
    ///
    /// If no `ORDER BY` is specified, `ordering_req` will be empty.
    pub ordering_req: &'a [PhysicalSortExpr],

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

    /// The physical expression of arguments the aggregate function takes.
    pub exprs: &'a [Arc<dyn PhysicalExpr>],
}

/// Factory that returns an accumulator for the given aggregate function.
pub type AccumulatorFactoryFunction =
    Arc<dyn Fn(AccumulatorArgs) -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// [`StateFieldsArgs`] contains information about the fields that an
/// aggregate function's accumulator should have. Used for `AggregateUDFImpl::state_fields`.
pub struct StateFieldsArgs<'a> {
    /// The name of the aggregate function.
    pub name: &'a str,

    /// The input types of the aggregate function.
    pub input_types: &'a [DataType],

    /// The return type of the aggregate function.
    pub return_type: &'a DataType,

    /// The ordering fields of the aggregate function.
    pub ordering_fields: &'a [Field],

    /// Whether the aggregate function is distinct.
    pub is_distinct: bool,
}
