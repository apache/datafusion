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
use datafusion::logical_expr::expr::InList;
use substrait::proto::expression::{RexType, ScalarFunction, SingularOrList};
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Expression, FunctionArgument};

pub fn from_in_list(
    producer: &mut impl SubstraitProducer,
    in_list: &InList,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let InList {
        expr,
        list,
        negated,
    } = in_list;
    let substrait_list = list
        .iter()
        .map(|x| producer.handle_expr(x, schema))
        .collect::<datafusion::common::Result<Vec<Expression>>>()?;
    let substrait_expr = producer.handle_expr(expr, schema)?;

    let substrait_or_list = Expression {
        rex_type: Some(RexType::SingularOrList(Box::new(SingularOrList {
            value: Some(Box::new(substrait_expr)),
            options: substrait_list,
        }))),
    };

    if *negated {
        let function_anchor = producer.register_function("not".to_string());

        #[expect(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_or_list)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_or_list)
    }
}
