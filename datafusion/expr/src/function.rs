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

//! Function module contains typing and signature for built-in and user defined functions.

use crate::ColumnarValue;
use crate::{Expr, PartitionEvaluator};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use std::sync::Arc;

pub use datafusion_functions_aggregate_common::accumulator::{
    AccumulatorArgs, AccumulatorFactoryFunction, StateFieldsArgs,
};

#[derive(Debug, Clone, Copy)]
pub enum Hint {
    /// Indicates the argument needs to be padded if it is scalar
    Pad,
    /// Indicates the argument can be converted to an array of length 1
    AcceptsSingular,
}

/// Scalar function
///
/// The Fn param is the wrapped function but be aware that the function will
/// be passed with the slice / vec of columnar values (either scalar or array)
/// with the exception of zero param function, where a singular element vec
/// will be passed. In that case the single element is a null array to indicate
/// the batch's row count (so that the generative zero-argument function can know
/// the result array size).
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

/// Factory that returns the functions's return type given the input argument types
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;

/// Factory that creates a PartitionEvaluator for the given window
/// function
pub type PartitionEvaluatorFactory =
    Arc<dyn Fn() -> Result<Box<dyn PartitionEvaluator>> + Send + Sync>;

/// Factory that returns the types used by an aggregator to serialize
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;

/// [crate::udaf::AggregateUDFImpl::simplify] simplifier closure
/// A closure with two arguments:
/// * 'aggregate_function': [crate::expr::AggregateFunction] for which simplified has been invoked
/// * 'info': [crate::simplify::SimplifyInfo]
///
/// closure returns simplified [Expr] or an error.
pub type AggregateFunctionSimplification = Box<
    dyn Fn(
        crate::expr::AggregateFunction,
        &dyn crate::simplify::SimplifyInfo,
    ) -> Result<Expr>,
>;

/// [crate::udwf::WindowUDFImpl::simplify] simplifier closure
/// A closure with two arguments:
/// * 'window_function': [crate::expr::WindowFunction] for which simplified has been invoked
/// * 'info': [crate::simplify::SimplifyInfo]
///
/// closure returns simplified [Expr] or an error.
pub type WindowFunctionSimplification = Box<
    dyn Fn(
        crate::expr::WindowFunction,
        &dyn crate::simplify::SimplifyInfo,
    ) -> Result<Expr>,
>;
