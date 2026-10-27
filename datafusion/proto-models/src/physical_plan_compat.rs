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

//! Conversions from the deprecated flat scan messages (`ParquetScanExecNode`,
//! `CsvScanExecNode`, …) to the format-specific part of the layered
//! `FileSourceNode`.
//!
//! Each conversion maps only the format fields; the shared `base_conf` is
//! handled by the caller. The legacy messages are destructured exhaustively so
//! that adding a field to one of them fails to compile until it is mapped here.

use crate::protobuf::{
    ArrowScanExecNode, ArrowSourceNode, AvroScanExecNode, AvroSourceNode,
    CsvScanExecNode, CsvSourceNode, JsonScanExecNode, JsonSourceNode,
    ParquetScanExecNode, ParquetSourceNode, csv_scan_exec_node, csv_source_node,
};

impl From<&ParquetScanExecNode> for ParquetSourceNode {
    fn from(scan: &ParquetScanExecNode) -> Self {
        let ParquetScanExecNode {
            base_conf: _,
            predicate,
            parquet_options,
            sort_order_for_reorder,
            reverse_row_groups,
            metadata_size_hint,
        } = scan;
        Self {
            predicate: predicate.clone(),
            parquet_options: parquet_options.clone(),
            sort_order_for_reorder: sort_order_for_reorder.clone(),
            reverse_row_groups: *reverse_row_groups,
            metadata_size_hint: *metadata_size_hint,
        }
    }
}

impl From<&CsvScanExecNode> for CsvSourceNode {
    fn from(scan: &CsvScanExecNode) -> Self {
        let CsvScanExecNode {
            base_conf: _,
            has_header,
            delimiter,
            quote,
            optional_escape,
            optional_comment,
            newlines_in_values,
            truncate_rows,
            terminator,
        } = scan;
        Self {
            has_header: *has_header,
            delimiter: delimiter.clone(),
            quote: quote.clone(),
            optional_escape: optional_escape.as_ref().map(
                |csv_scan_exec_node::OptionalEscape::Escape(e)| {
                    csv_source_node::OptionalEscape::Escape(e.clone())
                },
            ),
            optional_comment: optional_comment.as_ref().map(
                |csv_scan_exec_node::OptionalComment::Comment(c)| {
                    csv_source_node::OptionalComment::Comment(c.clone())
                },
            ),
            newlines_in_values: *newlines_in_values,
            truncate_rows: *truncate_rows,
            terminator: terminator.clone(),
        }
    }
}

impl From<&JsonScanExecNode> for JsonSourceNode {
    fn from(scan: &JsonScanExecNode) -> Self {
        let JsonScanExecNode {
            base_conf: _,
            newline_delimited,
        } = scan;
        Self {
            newline_delimited: *newline_delimited,
        }
    }
}

impl From<&AvroScanExecNode> for AvroSourceNode {
    fn from(scan: &AvroScanExecNode) -> Self {
        let AvroScanExecNode { base_conf: _ } = scan;
        Self {}
    }
}

impl From<&ArrowScanExecNode> for ArrowSourceNode {
    fn from(scan: &ArrowScanExecNode) -> Self {
        let ArrowScanExecNode {
            base_conf: _,
            format,
        } = scan;
        Self { format: *format }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion_common::TableParquetOptions;
    use crate::protobuf::{
        FileScanExecConf, PhysicalExprNode, PhysicalSortExprNodeCollection,
    };

    #[test]
    fn parquet_scan_maps_format_fields() {
        let scan = ParquetScanExecNode {
            base_conf: Some(FileScanExecConf::default()),
            predicate: Some(PhysicalExprNode::default()),
            parquet_options: Some(TableParquetOptions::default()),
            sort_order_for_reorder: Some(PhysicalSortExprNodeCollection::default()),
            reverse_row_groups: true,
            metadata_size_hint: Some(42),
        };
        assert_eq!(
            ParquetSourceNode::from(&scan),
            ParquetSourceNode {
                predicate: Some(PhysicalExprNode::default()),
                parquet_options: Some(TableParquetOptions::default()),
                sort_order_for_reorder: Some(PhysicalSortExprNodeCollection::default()),
                reverse_row_groups: true,
                metadata_size_hint: Some(42),
            }
        );
    }

    #[test]
    fn csv_scan_maps_format_fields() {
        let scan = CsvScanExecNode {
            base_conf: Some(FileScanExecConf::default()),
            has_header: true,
            delimiter: ";".to_string(),
            quote: "'".to_string(),
            optional_escape: Some(csv_scan_exec_node::OptionalEscape::Escape(
                "\\".to_string(),
            )),
            optional_comment: Some(csv_scan_exec_node::OptionalComment::Comment(
                "#".to_string(),
            )),
            newlines_in_values: true,
            truncate_rows: true,
            terminator: Some(b"\n".to_vec()),
        };
        assert_eq!(
            CsvSourceNode::from(&scan),
            CsvSourceNode {
                has_header: true,
                delimiter: ";".to_string(),
                quote: "'".to_string(),
                optional_escape: Some(csv_source_node::OptionalEscape::Escape(
                    "\\".to_string()
                )),
                optional_comment: Some(csv_source_node::OptionalComment::Comment(
                    "#".to_string()
                )),
                newlines_in_values: true,
                truncate_rows: true,
                terminator: Some(b"\n".to_vec()),
            }
        );
    }

    #[test]
    fn json_and_arrow_scans_map_format_fields() {
        let json = JsonScanExecNode {
            base_conf: None,
            newline_delimited: Some(false),
        };
        assert_eq!(
            JsonSourceNode::from(&json),
            JsonSourceNode {
                newline_delimited: Some(false)
            }
        );
        let arrow = ArrowScanExecNode {
            base_conf: None,
            format: 1,
        };
        assert_eq!(ArrowSourceNode::from(&arrow), ArrowSourceNode { format: 1 });
        assert_eq!(
            AvroSourceNode::from(&AvroScanExecNode { base_conf: None }),
            AvroSourceNode {}
        );
    }
}
