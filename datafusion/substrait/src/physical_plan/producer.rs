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

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use std::collections::HashMap;
use substrait::proto::expression::mask_expression::{StructItem, StructSelect};
use substrait::proto::expression::MaskExpression;
use substrait::proto::r#type::{
    Boolean, Fp64, Kind, Nullability, String as SubstraitString, Struct, I64,
};
use substrait::proto::read_rel::local_files::file_or_files::ParquetReadOptions;
use substrait::proto::read_rel::local_files::file_or_files::{FileFormat, PathType};
use substrait::proto::read_rel::local_files::FileOrFiles;
use substrait::proto::read_rel::LocalFiles;
use substrait::proto::read_rel::ReadType;
use substrait::proto::rel::RelType;
use substrait::proto::ReadRel;
use substrait::proto::Rel;
use substrait::proto::{extensions, NamedStruct, Type};

/// Convert DataFusion ExecutionPlan to Substrait Rel
pub fn to_substrait_rel(
    plan: &dyn ExecutionPlan,
    _extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> Result<Box<Rel>> {
    if let Some(scan) = plan.as_any().downcast_ref::<ParquetExec>() {
        let base_config = scan.base_config();
        let mut substrait_files = vec![];
        for (partition_index, files) in base_config.file_groups.iter().enumerate() {
            for file in files {
                substrait_files.push(FileOrFiles {
                    partition_index: partition_index.try_into().unwrap(),
                    start: 0,
                    length: file.object_meta.size as u64,
                    path_type: Some(PathType::UriPath(
                        file.object_meta.location.as_ref().to_string(),
                    )),
                    file_format: Some(FileFormat::Parquet(ParquetReadOptions {})),
                });
            }
        }

        let mut names = vec![];
        let mut types = vec![];

        for field in base_config.file_schema.fields.iter() {
            match to_substrait_type(field.data_type(), field.is_nullable()) {
                Ok(t) => {
                    names.push(field.name().clone());
                    types.push(t);
                }
                Err(e) => return Err(e),
            }
        }

        let type_info = Struct {
            types,
            // FIXME: duckdb doesn't set this field, keep it as default variant 0.
            // https://github.com/duckdb/substrait/blob/b6f56643cb11d52de0e32c24a01dfd5947df62be/src/to_substrait.cpp#L1106-L1127
            type_variation_reference: 0,
            nullability: Nullability::Required.into(),
        };

        let mut select_struct = None;
        if let Some(projection) = base_config.projection.as_ref() {
            let struct_items = projection
                .iter()
                .map(|index| StructItem {
                    field: *index as i32,
                    // FIXME: duckdb sets this to None, but it's not clear why.
                    // https://github.com/duckdb/substrait/blob/b6f56643cb11d52de0e32c24a01dfd5947df62be/src/to_substrait.cpp#L1191
                    child: None,
                })
                .collect();

            select_struct = Some(StructSelect { struct_items });
        }

        Ok(Box::new(Rel {
            rel_type: Some(RelType::Read(Box::new(ReadRel {
                common: None,
                base_schema: Some(NamedStruct {
                    names,
                    r#struct: Some(type_info),
                }),
                filter: None,
                best_effort_filter: None,
                projection: Some(MaskExpression {
                    select: select_struct,
                    // FIXME: duckdb set this to true, but it's not clear why.
                    // https://github.com/duckdb/substrait/blob/b6f56643cb11d52de0e32c24a01dfd5947df62be/src/to_substrait.cpp#L1186.
                    maintain_singular_struct: true,
                }),
                advanced_extension: None,
                read_type: Some(ReadType::LocalFiles(LocalFiles {
                    items: substrait_files,
                    advanced_extension: None,
                })),
            }))),
        }))
    } else {
        Err(DataFusionError::Substrait(format!(
            "Unsupported plan in Substrait physical plan producer: {}",
            displayable(plan).one_line()
        )))
    }
}

// see https://github.com/duckdb/substrait/blob/b6f56643cb11d52de0e32c24a01dfd5947df62be/src/to_substrait.cpp#L954-L1094.
fn to_substrait_type(data_type: &DataType, nullable: bool) -> Result<Type> {
    let nullability = if nullable {
        Nullability::Nullable.into()
    } else {
        Nullability::Required.into()
    };

    match data_type {
        DataType::Boolean => Ok(Type {
            kind: Some(Kind::Bool(Boolean {
                type_variation_reference: 0,
                nullability,
            })),
        }),
        DataType::Int64 => Ok(Type {
            kind: Some(Kind::I64(I64 {
                type_variation_reference: 0,
                nullability,
            })),
        }),
        DataType::Float64 => Ok(Type {
            kind: Some(Kind::Fp64(Fp64 {
                type_variation_reference: 0,
                nullability,
            })),
        }),
        DataType::Utf8 => Ok(Type {
            kind: Some(Kind::String(SubstraitString {
                type_variation_reference: 0,
                nullability,
            })),
        }),
        _ => Err(DataFusionError::Substrait(format!(
            "Logical type {data_type} not implemented as substrait type"
        ))),
    }
}
