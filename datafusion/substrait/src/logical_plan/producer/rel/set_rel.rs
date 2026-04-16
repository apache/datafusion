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
use datafusion::logical_expr::Union;
use substrait::proto::rel::RelType;
use substrait::proto::{Rel, SetRel, set_rel};

pub fn from_union(
    producer: &mut impl SubstraitProducer,
    union: &Union,
) -> datafusion::common::Result<Box<Rel>> {
    let input_rels = union
        .inputs
        .iter()
        .map(|input| producer.handle_plan(input.as_ref()))
        .collect::<datafusion::common::Result<Vec<_>>>()?
        .into_iter()
        .map(|ptr| *ptr)
        .collect();
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Set(SetRel {
            common: None,
            inputs: input_rels,
            op: set_rel::SetOp::UnionAll as i32, // UNION DISTINCT gets translated to AGGREGATION + UNION ALL
            advanced_extension: None,
        })),
    }))
}
