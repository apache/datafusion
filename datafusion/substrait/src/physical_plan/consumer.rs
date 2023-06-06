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

use async_recursion::async_recursion;
use chrono::DateTime;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use object_store::ObjectMeta;
use std::collections::HashMap;
use std::sync::Arc;
use substrait::proto::read_rel::local_files::file_or_files::PathType;
use substrait::proto::{
    expression::MaskExpression, read_rel::ReadType, rel::RelType, Rel,
};

/// Convert Substrait Rel to DataFusion ExecutionPlan
#[async_recursion]
pub async fn from_substrait_rel(
    _ctx: &mut SessionContext,
    rel: &Rel,
    _extensions: &HashMap<u32, &String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    match &rel.rel_type {
        Some(RelType::Read(read)) => {
            if read.filter.is_some() || read.best_effort_filter.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "Read with filter is not supported".to_string(),
                ));
            }
            if read.base_schema.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "Read with schema is not supported".to_string(),
                ));
            }
            if read.advanced_extension.is_some() {
                return Err(DataFusionError::NotImplemented(
                    "Read with AdvancedExtension is not supported".to_string(),
                ));
            }
            match &read.as_ref().read_type {
                Some(ReadType::LocalFiles(files)) => {
                    let mut file_groups = vec![];

                    for file in &files.items {
                        let path = if let Some(path_type) = &file.path_type {
                            match path_type {
                                PathType::UriPath(path) => Ok(path.clone()),
                                PathType::UriPathGlob(path) => Ok(path.clone()),
                                PathType::UriFile(path) => Ok(path.clone()),
                                PathType::UriFolder(path) => Ok(path.clone()),
                            }
                        } else {
                            Err(DataFusionError::Substrait(
                                "Missing PathType".to_string(),
                            ))
                        }?;

                        // TODO substrait plans do not have `last_modified` or `size` but `ObjectMeta`
                        // requires them both - perhaps we can change the object-store crate
                        // to make these optional? We cannot guarantee that we have access to the
                        // files to get this information, depending on how this library is being
                        // used
                        let last_modified = DateTime::parse_from_str(
                            "1970 Jan 1 00:00:00.000 +0000",
                            "%Y %b %d %H:%M:%S%.3f %z",
                        )
                        .unwrap();
                        let size = 0;

                        let partitioned_file = PartitionedFile {
                            object_meta: ObjectMeta {
                                last_modified: last_modified.into(),
                                location: path.into(),
                                size,
                                e_tag: None,
                            },
                            partition_values: vec![],
                            range: None,
                            extensions: None,
                        };

                        let part_index = file.partition_index as usize;
                        while part_index >= file_groups.len() {
                            file_groups.push(vec![]);
                        }
                        file_groups[part_index].push(partitioned_file)
                    }

                    let mut base_config = FileScanConfig {
                        object_store_url: ObjectStoreUrl::local_filesystem(),
                        file_schema: Arc::new(Schema::empty()),
                        file_groups,
                        statistics: Default::default(),
                        projection: None,
                        limit: None,
                        table_partition_cols: vec![],
                        output_ordering: vec![],
                        infinite_source: false,
                    };

                    if let Some(MaskExpression { select, .. }) = &read.projection {
                        if let Some(projection) = &select.as_ref() {
                            let column_indices: Vec<usize> = projection
                                .struct_items
                                .iter()
                                .map(|item| item.field as usize)
                                .collect();
                            base_config.projection = Some(column_indices);
                        }
                    }

                    Ok(Arc::new(ParquetExec::new(base_config, None, None))
                        as Arc<dyn ExecutionPlan>)
                }
                _ => Err(DataFusionError::NotImplemented(
                    "Only LocalFile reads are supported when parsing physical"
                        .to_string(),
                )),
            }
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported RelType: {:?}",
            rel.rel_type
        ))),
    }
}
