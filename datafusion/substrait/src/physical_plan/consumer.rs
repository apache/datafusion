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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{not_impl_err, substrait_err};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::variation_const::{
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, LARGE_CONTAINER_TYPE_VARIATION_REF,
    VIEW_CONTAINER_TYPE_VARIATION_REF,
};
use async_recursion::async_recursion;
use chrono::DateTime;
use datafusion::datasource::memory::DataSourceExec;
use object_store::ObjectMeta;
use substrait::proto::r#type::{Kind, Nullability};
use substrait::proto::read_rel::local_files::file_or_files::PathType;
use substrait::proto::Type;
use substrait::proto::{
    expression::MaskExpression, read_rel::ReadType, rel::RelType, Rel,
};

/// Convert Substrait Rel to DataFusion ExecutionPlan
#[async_recursion]
pub async fn from_substrait_rel(
    _ctx: &SessionContext,
    rel: &Rel,
    _extensions: &HashMap<u32, &String>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut base_config_builder;

    let source = Arc::new(ParquetSource::default());
    match &rel.rel_type {
        Some(RelType::Read(read)) => {
            if read.filter.is_some() || read.best_effort_filter.is_some() {
                return not_impl_err!("Read with filter is not supported");
            }

            if read.advanced_extension.is_some() {
                return not_impl_err!("Read with AdvancedExtension is not supported");
            }

            let Some(schema) = read.base_schema.as_ref() else {
                return substrait_err!("Missing base schema in the read");
            };

            let Some(r#struct) = schema.r#struct.as_ref() else {
                return substrait_err!("Missing struct in the schema");
            };

            match schema
                .names
                .iter()
                .zip(r#struct.types.iter())
                .map(|(name, r#type)| to_field(name, r#type))
                .collect::<Result<Vec<Field>>>()
            {
                Ok(fields) => {
                    base_config_builder = FileScanConfigBuilder::new(
                        ObjectStoreUrl::local_filesystem(),
                        Arc::new(Schema::new(fields)),
                        source,
                    );
                }
                Err(e) => return Err(e),
            };

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
                                version: None,
                            },
                            partition_values: vec![],
                            range: None,
                            statistics: None,
                            extensions: None,
                            metadata_size_hint: None,
                        };

                        let part_index = file.partition_index as usize;
                        while part_index >= file_groups.len() {
                            file_groups.push(FileGroup::default());
                        }
                        file_groups[part_index].push(partitioned_file)
                    }

                    base_config_builder =
                        base_config_builder.with_file_groups(file_groups);

                    if let Some(MaskExpression { select, .. }) = &read.projection {
                        if let Some(projection) = &select.as_ref() {
                            let column_indices: Vec<usize> = projection
                                .struct_items
                                .iter()
                                .map(|item| item.field as usize)
                                .collect();
                            base_config_builder =
                                base_config_builder.with_projection(Some(column_indices));
                        }
                    }

                    Ok(
                        DataSourceExec::from_data_source(base_config_builder.build())
                            as Arc<dyn ExecutionPlan>,
                    )
                }
                _ => not_impl_err!(
                    "Only LocalFile reads are supported when parsing physical"
                ),
            }
        }
        _ => not_impl_err!("Unsupported RelType: {:?}", rel.rel_type),
    }
}

fn to_field(name: &String, r#type: &Type) -> Result<Field> {
    let Some(kind) = r#type.kind.as_ref() else {
        return substrait_err!("Missing kind in the type with name {}", name);
    };

    let mut nullable = false;
    let data_type = match kind {
        Kind::Bool(boolean) => {
            nullable = is_nullable(boolean.nullability);
            Ok(DataType::Boolean)
        }
        Kind::I64(i64) => {
            nullable = is_nullable(i64.nullability);
            Ok(DataType::Int64)
        }
        Kind::Fp64(fp64) => {
            nullable = is_nullable(fp64.nullability);
            Ok(DataType::Float64)
        }
        Kind::String(string) => {
            nullable = is_nullable(string.nullability);
            match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeUtf8),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8View),
                _ => substrait_err!(
                    "Invalid type variation found for substrait string type class: {}",
                    string.type_variation_reference
                ),
            }
        }
        Kind::Binary(binary) => {
            nullable = is_nullable(binary.nullability);
            match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Binary),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeBinary),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::BinaryView),
                _ => substrait_err!(
                    "Invalid type variation found for substrait binary type class: {}",
                    binary.type_variation_reference
                ),
            }
        }
        _ => substrait_err!(
            "Unsupported kind: {:?} in the type with name {}",
            kind,
            name
        ),
    }?;

    Ok(Field::new(name, data_type, nullable))
}

fn is_nullable(nullability: i32) -> bool {
    let Ok(nullability) = Nullability::try_from(nullability) else {
        return true;
    };

    match nullability {
        Nullability::Nullable | Nullability::Unspecified => true,
        Nullability::Required => false,
    }
}
