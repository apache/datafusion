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

use crate::logical_plan::consumer::SubstraitConsumer;
use crate::logical_plan::recursive::decode_recursive_query_detail;
use datafusion::common::substrait_err;
use datafusion::logical_expr::{LogicalPlan, RecursiveQuery};
use std::sync::Arc;
use substrait::proto::ExtensionMultiRel;

/// Deserializes Substrait ExtensionMultiRel back into DataFusion RecursiveQuery.
///
/// RecursiveQuery is encoded as ExtensionMultiRel with exactly two inputs:
///
/// - `inputs[0]`: static_term
/// - `inputs[1]`: recursive_term
///
/// The detail field contains the encoded name and is_distinct metadata.
pub async fn from_recursive_query_rel(
    consumer: &impl SubstraitConsumer,
    rel: &ExtensionMultiRel,
) -> datafusion::common::Result<LogicalPlan> {
    // Validate structure
    if rel.inputs.len() != 2 {
        return substrait_err!(
            "RecursiveQuery extension must have exactly 2 inputs, found {}",
            rel.inputs.len()
        );
    }

    let Some(detail) = &rel.detail else {
        return substrait_err!("RecursiveQuery extension missing detail");
    };

    // Decode metadata
    let (name, is_distinct) = decode_recursive_query_detail(&detail.value)?;

    // Convert child plans
    let static_term = Arc::new(consumer.consume_rel(&rel.inputs[0]).await?);
    let recursive_term = Arc::new(consumer.consume_rel(&rel.inputs[1]).await?);

    Ok(LogicalPlan::RecursiveQuery(RecursiveQuery {
        name,
        static_term,
        recursive_term,
        is_distinct,
    }))
}
