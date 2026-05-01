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

//! Conversions between `datafusion-proto-common` types and
//! `datafusion-datasource` types. Enabled by the `proto` feature.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion_common::proto::proto_error;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::dml::InsertOp;
use datafusion_proto_common::protobuf;
use object_store::ObjectMeta;
use object_store::path::Path;

use crate::file_groups::FileGroup;
use crate::file_sink_config::FileSinkConfig;
use crate::{FileRange, ListingTableUrl, PartitionedFile};

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        let mut pf = PartitionedFile::new_from_meta(ObjectMeta {
            location: Path::parse(val.path.as_str())
                .map_err(|e| proto_error(format!("Invalid object_store path: {e}")))?,
            last_modified: Utc.timestamp_nanos(val.last_modified_ns as i64),
            size: val.size,
            e_tag: None,
            version: None,
        })
        .with_partition_values(
            val.partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        );
        if let Some(range) = val.range.as_ref() {
            let file_range: FileRange = range.try_into()?;
            pf = pf.with_range(file_range.start, file_range.end);
        }
        if let Some(proto_stats) = val.statistics.as_ref() {
            pf = pf.with_statistics(Arc::new(proto_stats.try_into()?));
        }
        Ok(pf)
    }
}

impl TryFrom<&PartitionedFile> for protobuf::PartitionedFile {
    type Error = DataFusionError;

    fn try_from(pf: &PartitionedFile) -> Result<Self> {
        let last_modified = pf.object_meta.last_modified;
        let last_modified_ns = last_modified.timestamp_nanos_opt().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Invalid timestamp on PartitionedFile::ObjectMeta: {last_modified}"
            ))
        })? as u64;
        Ok(protobuf::PartitionedFile {
            path: pf.object_meta.location.as_ref().to_owned(),
            size: pf.object_meta.size,
            last_modified_ns,
            partition_values: pf
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: pf
                .range
                .as_ref()
                .map(protobuf::FileRange::try_from)
                .transpose()?,
            statistics: pf.statistics.as_ref().map(|s| s.as_ref().into()),
        })
    }
}

impl TryFrom<&protobuf::FileRange> for FileRange {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::FileRange) -> Result<Self, Self::Error> {
        Ok(FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&FileRange> for protobuf::FileRange {
    type Error = DataFusionError;

    fn try_from(value: &FileRange) -> Result<Self> {
        Ok(protobuf::FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for FileGroup {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::FileGroup) -> Result<Self, Self::Error> {
        let files = val
            .files
            .iter()
            .map(PartitionedFile::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileGroup::new(files))
    }
}

impl TryFrom<&FileGroup> for protobuf::FileGroup {
    type Error = DataFusionError;

    fn try_from(gr: &FileGroup) -> Result<Self, Self::Error> {
        Ok(protobuf::FileGroup {
            files: gr
                .files()
                .iter()
                .map(protobuf::PartitionedFile::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<&protobuf::FileSinkConfig> for FileSinkConfig {
    type Error = DataFusionError;

    fn try_from(conf: &protobuf::FileSinkConfig) -> Result<Self, Self::Error> {
        let file_group = FileGroup::new(
            conf.file_groups
                .iter()
                .map(PartitionedFile::try_from)
                .collect::<Result<Vec<_>>>()?,
        );
        let table_paths = conf
            .table_paths
            .iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<_>>>()?;
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|protobuf::PartitionColumn { name, arrow_type }| {
                let arrow_type = arrow_type.as_ref().ok_or_else(|| {
                    proto_error("Missing required field arrow_type in PartitionColumn")
                })?;
                let data_type: arrow::datatypes::DataType = arrow_type.try_into()?;
                Ok((name.clone(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;
        let insert_op = match conf.insert_op() {
            protobuf::InsertOp::Append => InsertOp::Append,
            protobuf::InsertOp::Overwrite => InsertOp::Overwrite,
            protobuf::InsertOp::Replace => InsertOp::Replace,
        };
        let file_output_mode = match conf.file_output_mode() {
            protobuf::FileOutputMode::Automatic => {
                crate::file_sink_config::FileOutputMode::Automatic
            }
            protobuf::FileOutputMode::SingleFile => {
                crate::file_sink_config::FileOutputMode::SingleFile
            }
            protobuf::FileOutputMode::Directory => {
                crate::file_sink_config::FileOutputMode::Directory
            }
        };
        let output_schema = conf.output_schema.as_ref().ok_or_else(|| {
            proto_error("Missing required field output_schema in FileSinkConfig")
        })?;
        Ok(Self {
            original_url: String::default(),
            object_store_url: datafusion_execution::object_store::ObjectStoreUrl::parse(
                &conf.object_store_url,
            )?,
            file_group,
            table_paths,
            output_schema: Arc::new(output_schema.try_into()?),
            table_partition_cols,
            insert_op,
            keep_partition_by_columns: conf.keep_partition_by_columns,
            file_extension: conf.file_extension.clone(),
            file_output_mode,
        })
    }
}

impl TryFrom<&FileSinkConfig> for protobuf::FileSinkConfig {
    type Error = DataFusionError;

    fn try_from(conf: &FileSinkConfig) -> Result<Self, Self::Error> {
        let file_groups = conf
            .file_group
            .iter()
            .map(protobuf::PartitionedFile::try_from)
            .collect::<Result<Vec<_>>>()?;
        let table_paths = conf
            .table_paths
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|(name, data_type)| {
                Ok::<_, DataFusionError>(protobuf::PartitionColumn {
                    name: name.to_owned(),
                    arrow_type: Some(data_type.try_into()?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let insert_op = match conf.insert_op {
            InsertOp::Append => protobuf::InsertOp::Append,
            InsertOp::Overwrite => protobuf::InsertOp::Overwrite,
            InsertOp::Replace => protobuf::InsertOp::Replace,
        };
        let file_output_mode = match conf.file_output_mode {
            crate::file_sink_config::FileOutputMode::Automatic => {
                protobuf::FileOutputMode::Automatic
            }
            crate::file_sink_config::FileOutputMode::SingleFile => {
                protobuf::FileOutputMode::SingleFile
            }
            crate::file_sink_config::FileOutputMode::Directory => {
                protobuf::FileOutputMode::Directory
            }
        };
        Ok(protobuf::FileSinkConfig {
            object_store_url: conf.object_store_url.to_string(),
            file_groups,
            table_paths,
            output_schema: Some(conf.output_schema.as_ref().try_into()?),
            table_partition_cols,
            insert_op: insert_op.into(),
            keep_partition_by_columns: conf.keep_partition_by_columns,
            file_extension: conf.file_extension.clone(),
            file_output_mode: file_output_mode.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitioned_file_path_roundtrip_percent_encoded() {
        let path_str = "foo/foo%2Fbar/baz%252Fqux";
        let pf = PartitionedFile::new_from_meta(ObjectMeta {
            location: Path::parse(path_str).unwrap(),
            last_modified: Utc.timestamp_nanos(1_000),
            size: 42,
            e_tag: None,
            version: None,
        });

        let proto = protobuf::PartitionedFile::try_from(&pf).unwrap();
        assert_eq!(proto.path, path_str);

        let pf2 = PartitionedFile::try_from(&proto).unwrap();
        assert_eq!(pf2.object_meta.location.as_ref(), path_str);
        assert_eq!(pf2.object_meta.location, pf.object_meta.location);
        assert_eq!(pf2.object_meta.size, pf.object_meta.size);
        assert_eq!(pf2.object_meta.last_modified, pf.object_meta.last_modified);
    }

    #[test]
    fn partitioned_file_from_proto_invalid_path() {
        let proto = protobuf::PartitionedFile {
            path: "foo//bar".to_string(),
            size: 1,
            last_modified_ns: 0,
            partition_values: vec![],
            range: None,
            statistics: None,
        };

        let err = PartitionedFile::try_from(&proto).unwrap_err();
        assert!(err.to_string().contains("Invalid object_store path"));
    }
}
