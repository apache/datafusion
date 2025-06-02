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
use datafusion::logical_expr::Filter;
use substrait::proto::rel::RelType;
use substrait::proto::{FilterRel, Rel};

pub fn from_filter(
    producer: &mut impl SubstraitProducer,
    filter: &Filter,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(filter.input.as_ref())?;
    let filter_expr = producer.handle_expr(&filter.predicate, filter.input.schema())?;
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Filter(Box::new(FilterRel {
            common: None,
            input: Some(input),
            condition: Some(Box::new(filter_expr)),
            advanced_extension: None,
        }))),
    }))
}
