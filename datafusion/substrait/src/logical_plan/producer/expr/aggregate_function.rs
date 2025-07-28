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

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::expr;
use datafusion::logical_expr::expr::AggregateFunctionParams;
use substrait::proto::aggregate_function::AggregationInvocation;
use substrait::proto::aggregate_rel::Measure;
use substrait::proto::function_argument::ArgType;
use substrait::proto::sort_field::{SortDirection, SortKind};
use substrait::proto::{
    AggregateFunction, AggregationPhase, FunctionArgument, SortField,
};

pub fn from_aggregate_function(
    producer: &mut impl SubstraitProducer,
    agg_fn: &expr::AggregateFunction,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Measure> {
    let expr::AggregateFunction {
        func,
        params:
            AggregateFunctionParams {
                args,
                distinct,
                filter,
                order_by,
                null_treatment: _null_treatment,
            },
    } = agg_fn;
    let sorts = order_by
        .iter()
        .map(|expr| to_substrait_sort_field(producer, expr, schema))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }
    let function_anchor = producer.register_function(func.name().to_string());
    #[allow(deprecated)]
    Ok(Measure {
        measure: Some(AggregateFunction {
            function_reference: function_anchor,
            arguments,
            sorts,
            output_type: None,
            invocation: match distinct {
                true => AggregationInvocation::Distinct as i32,
                false => AggregationInvocation::All as i32,
            },
            phase: AggregationPhase::Unspecified as i32,
            args: vec![],
            options: vec![],
        }),
        filter: match filter {
            Some(f) => Some(producer.handle_expr(f, schema)?),
            None => None,
        },
    })
}

/// Converts sort expression to corresponding substrait `SortField`
fn to_substrait_sort_field(
    producer: &mut impl SubstraitProducer,
    sort: &expr::Sort,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<SortField> {
    let sort_kind = match (sort.asc, sort.nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(producer.handle_expr(&sort.expr, schema)?),
        sort_kind: Some(SortKind::Direction(sort_kind.into())),
    })
}
