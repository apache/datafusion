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

use crate::logical_plan::{
    producer::SubstraitProducer,
    recursive::{RECURSIVE_QUERY_TYPE_URL, encode_recursive_query_detail},
};
use datafusion::logical_expr::RecursiveQuery;
use pbjson_types::Any as ProtoAny;
use substrait::proto::{
    ExtensionMultiRel, Rel, RelCommon,
    rel_common::{self, EmitKind},
};
/// Serializes DataFusion RecursiveQuery into Substrait ExtensionMultiRel.
///
/// RecursiveQuery has two child plans (static_term and recursive_term) plus metadata
/// (name and is_distinct), so we use ExtensionMultiRel to hold both inputs.
pub fn from_recursive_query(
    producer: &mut impl SubstraitProducer,
    recursive_query: &RecursiveQuery,
) -> datafusion::common::Result<Box<Rel>> {
    // Convert both child plans
    let static_term_rel = producer.handle_plan(&recursive_query.static_term)?;
    let recursive_term_rel = producer.handle_plan(&recursive_query.recursive_term)?;

    // Encode metadata into a simple protobuf message
    let detail_bytes = encode_recursive_query_detail(
        &recursive_query.name,
        recursive_query.is_distinct,
    )?;

    let detail = ProtoAny {
        type_url: RECURSIVE_QUERY_TYPE_URL.to_string(),
        value: detail_bytes.into(),
    };

    // Use ExtensionMultiRel with two inputs
    let rel_type = substrait::proto::rel::RelType::ExtensionMulti(ExtensionMultiRel {
        common: Some(RelCommon {
            emit_kind: Some(EmitKind::Direct(rel_common::Direct {})),
            ..Default::default()
        }),
        detail: Some(detail),
        inputs: vec![*static_term_rel, *recursive_term_rel],
    });

    Ok(Box::new(Rel {
        rel_type: Some(rel_type),
    }))
}

// encode/decode helpers and type URLs live in `logical_plan::recursive`
