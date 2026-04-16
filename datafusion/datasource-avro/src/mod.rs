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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! An [Avro](https://avro.apache.org/) based [`FileSource`](datafusion_datasource::file::FileSource) implementation and related functionality.

pub mod file_format;
pub mod source;

use arrow::datatypes::{DataType, Field, Fields, Schema, UnionFields};
pub use arrow_avro;
use arrow_avro::reader::ReaderBuilder;
pub use file_format::*;
use std::io::{BufReader, Read};
use std::sync::Arc;

/// Read Avro schema given a reader
pub fn read_avro_schema_from_reader<R: Read>(
    reader: &mut R,
) -> datafusion_common::Result<Schema> {
    let avro_reader = ReaderBuilder::new().build(BufReader::new(reader))?;
    // Avro readers perform strict schema resolution rules (e.g. record identity checks)
    // that are stricter than DataFusion's table schema handling needs for inferred schemas.
    // Drop metadata from inferred schemas so runtime batches and inferred table schemas
    // compare consistently without requiring strict Avro metadata identity.
    Ok(strip_metadata_from_schema(avro_reader.schema().as_ref()))
}

fn strip_metadata_from_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields
        .into_iter()
        .map(|f| Arc::new(strip_metadata_from_field(f.as_ref())))
        .collect::<Fields>();
    // Intentionally drop schema-level metadata
    Schema::new(fields)
}

fn strip_metadata_from_field(field: &Field) -> Field {
    // Intentionally drop field-level metadata
    Field::new(
        field.name(),
        strip_metadata_from_data_type(field.data_type()),
        field.is_nullable(),
    )
}

fn strip_metadata_from_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| Arc::new(strip_metadata_from_field(f.as_ref())))
                .collect(),
        ),
        DataType::List(field) => {
            DataType::List(Arc::new(strip_metadata_from_field(field.as_ref())))
        }
        DataType::LargeList(field) => {
            DataType::LargeList(Arc::new(strip_metadata_from_field(field.as_ref())))
        }
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
            Arc::new(strip_metadata_from_field(field.as_ref())),
            *size,
        ),
        DataType::Map(field, sorted) => {
            DataType::Map(Arc::new(strip_metadata_from_field(field.as_ref())), *sorted)
        }
        DataType::Union(fields, mode) => {
            let (type_ids, children): (Vec<_>, Vec<_>) = fields
                .iter()
                .map(|(type_id, field)| {
                    (type_id, Arc::new(strip_metadata_from_field(field.as_ref())))
                })
                .unzip();

            DataType::Union(
                UnionFields::try_new(type_ids, children)
                    .expect("existing union fields should remain valid"),
                *mode,
            )
        }
        _ => data_type.clone(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion_common::Result as DFResult;
    use datafusion_common::test_util::arrow_test_data;
    use std::fs::File;

    fn avro_test_file(name: &str) -> String {
        format!("{}/avro/{name}", arrow_test_data())
    }

    #[test]
    fn test_read_avro_schema_from_reader() -> DFResult<()> {
        let path = avro_test_file("alltypes_dictionary.avro");
        let mut file = File::open(&path)?;
        let file_schema = read_avro_schema_from_reader(&mut file)?;

        let expected_fields = vec![
            Field::new("id", DataType::Int32, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("tinyint_col", DataType::Int32, true),
            Field::new("smallint_col", DataType::Int32, true),
            Field::new("int_col", DataType::Int32, true),
            Field::new("bigint_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float32, true),
            Field::new("double_col", DataType::Float64, true),
            Field::new("date_string_col", DataType::Binary, true),
            Field::new("string_col", DataType::Binary, true),
            Field::new(
                "timestamp_col",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
        ];

        assert_eq!(file_schema.fields.len(), expected_fields.len());
        for (i, field) in file_schema.fields.iter().enumerate() {
            assert_eq!(field.as_ref(), &expected_fields[i]);
        }

        Ok(())
    }
}
