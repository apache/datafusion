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

use prost::Message;

/// Type URL to identify RecursiveQuery in Substrait extensions
pub const RECURSIVE_QUERY_TYPE_URL: &str = "datafusion.RecursiveQuery";

/// Type URL to identify recursive scan (work-table used for recursive CTEs)
pub const RECURSIVE_SCAN_TYPE_URL: &str = "datafusion.RecursiveScan";

/// Simple protobuf message to encode RecursiveQuery metadata
#[derive(Clone, PartialEq, prost::Message)]
struct RecursiveQueryDetail {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(bool, tag = "2")]
    is_distinct: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RecursiveScanDetail {
    #[prost(string, tag = "1")]
    pub name: String,
}

pub fn encode_recursive_query_detail(
    name: &str,
    is_distinct: bool,
) -> datafusion::common::Result<Vec<u8>> {
    if name.is_empty() {
        return datafusion::common::substrait_err!("RecursiveQuery name cannot be empty");
    }

    let detail = RecursiveQueryDetail {
        name: name.to_string(),
        is_distinct,
    };
    Ok(detail.encode_to_vec())
}

pub fn encode_recursive_scan_detail(name: &str) -> datafusion::common::Result<Vec<u8>> {
    if name.is_empty() {
        return datafusion::common::substrait_err!(
            "Recursive table name cannot be empty"
        );
    }

    let detail = RecursiveScanDetail {
        name: name.to_string(),
    };
    Ok(detail.encode_to_vec())
}

/// Decodes RecursiveQuery metadata from protobuf bytes
pub fn decode_recursive_query_detail(
    bytes: &[u8],
) -> datafusion::common::Result<(String, bool)> {
    let detail = RecursiveQueryDetail::decode(bytes).map_err(|e| {
        datafusion::common::DataFusionError::Substrait(format!(
            "Failed to decode RecursiveQueryDetail: {e}"
        ))
    })?;

    // Validate that name is not empty
    if detail.name.is_empty() {
        return datafusion::common::substrait_err!("RecursiveQuery name cannot be empty");
    }

    Ok((detail.name, detail.is_distinct))
}

pub fn decode_recursive_scan_detail(bytes: &[u8]) -> datafusion::common::Result<String> {
    let detail = RecursiveScanDetail::decode(bytes).map_err(|e| {
        datafusion::common::DataFusionError::Substrait(format!(
            "Failed to decode RecursiveScanDetail: {e}"
        ))
    })?;

    if detail.name.is_empty() {
        return datafusion::common::substrait_err!(
            "Recursive table name cannot be empty"
        );
    }

    Ok(detail.name)
}
