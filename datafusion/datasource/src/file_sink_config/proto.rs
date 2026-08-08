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

//! Protobuf conversion for the format-independent [`FileSinkConfig`].

use std::sync::Arc;

use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::dml::InsertOp;
use datafusion_proto_models::protobuf;

use crate::ListingTableUrl;
use crate::file_groups::FileGroup;
use crate::file_sink_config::{FileOutputMode, FileSinkConfig};

impl TryFrom<&FileSinkConfig> for protobuf::FileSinkConfig {
    type Error = DataFusionError;

    /// Serialize this shared file-sink configuration without format-specific
    /// writer options.
    fn try_from(config: &FileSinkConfig) -> Result<Self> {
        let file_groups = config
            .file_group
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let table_paths = config
            .table_paths
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let table_partition_cols = config
            .table_partition_cols
            .iter()
            .map(|(name, data_type)| {
                Ok(protobuf::PartitionColumn {
                    name: name.to_owned(),
                    arrow_type: Some(data_type.try_into()?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let insert_op = match config.insert_op {
            InsertOp::Append => protobuf::InsertOp::Append,
            InsertOp::Overwrite => protobuf::InsertOp::Overwrite,
            InsertOp::Replace => protobuf::InsertOp::Replace,
        };
        let file_output_mode = match config.file_output_mode {
            FileOutputMode::Automatic => protobuf::FileOutputMode::Automatic,
            FileOutputMode::SingleFile => protobuf::FileOutputMode::SingleFile,
            FileOutputMode::Directory => protobuf::FileOutputMode::Directory,
        };

        Ok(protobuf::FileSinkConfig {
            object_store_url: config.object_store_url.to_string(),
            file_groups,
            table_paths,
            output_schema: Some(config.output_schema.as_ref().try_into()?),
            table_partition_cols,
            keep_partition_by_columns: config.keep_partition_by_columns,
            insert_op: insert_op.into(),
            file_extension: config.file_extension.clone(),
            file_output_mode: file_output_mode.into(),
        })
    }
}

impl TryFrom<&protobuf::FileSinkConfig> for FileSinkConfig {
    type Error = DataFusionError;

    /// Reconstruct a shared file-sink configuration from protobuf.
    fn try_from(conf: &protobuf::FileSinkConfig) -> Result<Self> {
        let file_group = FileGroup::new(
            conf.file_groups
                .iter()
                .map(TryInto::try_into)
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
                let data_type = arrow_type
                    .as_ref()
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "PartitionColumn is missing required field 'arrow_type'"
                        )
                    })?
                    .try_into()?;
                Ok((name.clone(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;
        let insert_op = protobuf::InsertOp::try_from(conf.insert_op).map_err(|_| {
            internal_datafusion_err!(
                "Received a FileSinkConfig message with unknown InsertOp {}",
                conf.insert_op
            )
        })?;
        let insert_op = match insert_op {
            protobuf::InsertOp::Append => InsertOp::Append,
            protobuf::InsertOp::Overwrite => InsertOp::Overwrite,
            protobuf::InsertOp::Replace => InsertOp::Replace,
        };
        let file_output_mode = protobuf::FileOutputMode::try_from(conf.file_output_mode)
            .map_err(|_| {
                internal_datafusion_err!(
                    "Received a FileSinkConfig message with unknown FileOutputMode {}",
                    conf.file_output_mode
                )
            })?;
        let file_output_mode = match file_output_mode {
            protobuf::FileOutputMode::Automatic => FileOutputMode::Automatic,
            protobuf::FileOutputMode::SingleFile => FileOutputMode::SingleFile,
            protobuf::FileOutputMode::Directory => FileOutputMode::Directory,
        };
        let output_schema = conf.output_schema.as_ref().ok_or_else(|| {
            internal_datafusion_err!(
                "FileSinkConfig is missing required field 'output_schema'"
            )
        })?;

        Ok(Self {
            original_url: String::default(),
            object_store_url: ObjectStoreUrl::parse(&conf.object_store_url)?,
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;

    use super::*;

    fn valid_file_sink_config() -> protobuf::FileSinkConfig {
        protobuf::FileSinkConfig {
            object_store_url: ObjectStoreUrl::local_filesystem().to_string(),
            output_schema: Some(
                (&Schema::empty())
                    .try_into()
                    .expect("empty schema should serialize"),
            ),
            insert_op: protobuf::InsertOp::Append.into(),
            file_output_mode: protobuf::FileOutputMode::Automatic.into(),
            ..Default::default()
        }
    }

    fn assert_decode_error(
        mutate: impl FnOnce(&mut protobuf::FileSinkConfig),
        expected: impl AsRef<str>,
    ) {
        let mut conf = valid_file_sink_config();
        mutate(&mut conf);

        let error =
            FileSinkConfig::try_from(&conf).expect_err("invalid config should fail");
        match error {
            DataFusionError::Internal(message) => {
                let message = message
                    .split_once(DataFusionError::BACK_TRACE_SEP)
                    .map_or(message.as_str(), |(message, _)| message);
                assert_eq!(message, expected.as_ref());
            }
            error => panic!("expected internal error, got {error}"),
        }
    }

    #[test]
    fn rejects_unknown_insert_op() {
        assert_decode_error(
            |conf| conf.insert_op = i32::MAX,
            format!(
                "Received a FileSinkConfig message with unknown InsertOp {}",
                i32::MAX
            ),
        );
    }

    #[test]
    fn rejects_unknown_file_output_mode() {
        assert_decode_error(
            |conf| conf.file_output_mode = i32::MAX,
            format!(
                "Received a FileSinkConfig message with unknown FileOutputMode {}",
                i32::MAX
            ),
        );
    }

    #[test]
    fn rejects_missing_output_schema() {
        assert_decode_error(
            |conf| conf.output_schema = None,
            "FileSinkConfig is missing required field 'output_schema'",
        );
    }

    #[test]
    fn rejects_partition_column_without_arrow_type() {
        assert_decode_error(
            |conf| {
                conf.table_partition_cols.push(protobuf::PartitionColumn {
                    name: "partition".to_string(),
                    arrow_type: None,
                });
            },
            "PartitionColumn is missing required field 'arrow_type'",
        );
    }
}
