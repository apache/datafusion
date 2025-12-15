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
#![deny(clippy::allow_attributes)]

//! An [Avro](https://avro.apache.org/) based [`FileSource`](datafusion_datasource::file::FileSource) implementation and related functionality.

pub mod file_format;
pub mod source;

use arrow::datatypes::Schema;
pub use arrow_avro;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::SCHEMA_METADATA_KEY;
use datafusion_common::DataFusionError;
pub use file_format::*;
use std::io::{BufReader, Read};
use std::sync::Arc;

/// Read Avro schema given a reader
pub fn read_avro_schema_from_reader<R: Read>(
    reader: &mut R,
) -> datafusion_common::Result<Schema> {
    let avro_reader = ReaderBuilder::new().build(BufReader::new(reader))?;
    let schema_ref = avro_reader.schema();
    // Extract the raw Avro JSON schema from the OCF header.
    let raw_json = avro_reader
        .avro_header()
        .get(SCHEMA_METADATA_KEY.as_bytes())
        .map(|bytes| {
            std::str::from_utf8(bytes).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Invalid UTF-8 in Avro schema metadata ({SCHEMA_METADATA_KEY}): {e}"
                ))
            })
        })
        .transpose()?
        .map(str::to_owned);
    drop(avro_reader);
    if let Some(raw_json) = raw_json {
        let mut schema = Arc::unwrap_or_clone(schema_ref);
        // Insert the raw Avro JSON schema using `SCHEMA_METADATA_KEY`.
        // This should enable the avro schema metadata to be picked downstream.
        schema
            .metadata
            .insert(SCHEMA_METADATA_KEY.to_string(), raw_json);
        Ok(schema)
    } else {
        // Return error because Avro spec requires the Avro schema metadata to be present in the OCF header.
        Err(DataFusionError::Execution(format!(
            "Avro schema metadata ({SCHEMA_METADATA_KEY}) is missing from OCF header"
        )))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{BinaryArray, BooleanArray, Float64Array};
    use arrow::datatypes::DataType;
    use arrow_avro::reader::ReaderBuilder;
    use arrow_avro::schema::{AvroSchema, SCHEMA_METADATA_KEY};
    use datafusion_common::test_util::arrow_test_data;
    use datafusion_common::{DataFusionError, Result as DFResult};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;

    fn avro_test_file(name: &str) -> String {
        format!("{}/avro/{name}", arrow_test_data())
    }

    #[test]
    fn read_avro_schema_includes_avro_json_metadata() -> DFResult<()> {
        let path = avro_test_file("alltypes_plain.avro");
        let mut file = File::open(&path)?;
        let schema = read_avro_schema_from_reader(&mut file)?;
        let meta_json = schema
            .metadata()
            .get(SCHEMA_METADATA_KEY)
            .expect("schema metadata missing avro.schema entry");
        assert!(
            !meta_json.is_empty(),
            "avro.schema metadata should not be empty"
        );
        let mut raw = File::open(&path)?;
        let avro_reader = ReaderBuilder::new().build(BufReader::new(&mut raw))?;
        let header_json = avro_reader
            .avro_header()
            .get(SCHEMA_METADATA_KEY.as_bytes())
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .expect("missing avro.schema metadata in OCF header");
        assert_eq!(
            meta_json, header_json,
            "schema metadata avro.schema should match OCF header"
        );
        Ok(())
    }

    #[test]
    fn read_and_project_using_schema_metadata() -> DFResult<()> {
        let path = avro_test_file("alltypes_dictionary.avro");
        let mut file = File::open(&path)?;
        let file_schema = read_avro_schema_from_reader(&mut file)?;
        let projected_field_names = vec!["string_col", "double_col", "bool_col"];
        let avro_json = file_schema
            .metadata()
            .get(SCHEMA_METADATA_KEY)
            .expect("schema metadata missing avro.schema entry");
        let projected_avro_schema =
            build_projected_reader_schema(avro_json, &projected_field_names)?;
        let mut reader = ReaderBuilder::new()
            .with_reader_schema(projected_avro_schema)
            .with_batch_size(64)
            .build(BufReader::new(File::open(&path)?))?;
        let batch = reader.next().expect("no batch produced")?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());
        let schema = batch.schema();
        assert_eq!("string_col", schema.field(0).name());
        assert_eq!(&DataType::Binary, schema.field(0).data_type());
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("column 0 not BinaryArray");
        assert_eq!("0".as_bytes(), col.value(0));
        assert_eq!("1".as_bytes(), col.value(1));
        assert_eq!("double_col", schema.field(1).name());
        assert_eq!(&DataType::Float64, schema.field(1).data_type());
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("column 1 not Float64Array");
        assert_eq!(0.0, col.value(0));
        assert_eq!(10.1, col.value(1));
        assert_eq!("bool_col", schema.field(2).name());
        assert_eq!(&DataType::Boolean, schema.field(2).data_type());
        let col = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("column 2 not BooleanArray");
        assert!(col.value(0));
        assert!(!col.value(1));
        Ok(())
    }

    fn build_projected_reader_schema(
        avro_json: &str,
        projected_field_names: &[&str],
    ) -> DFResult<AvroSchema> {
        let mut schema_json: Value = serde_json::from_str(avro_json).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to parse Avro schema JSON from metadata: {e}"
            ))
        })?;
        let obj = schema_json.as_object_mut().ok_or_else(|| {
            DataFusionError::Execution(
                "Top-level Avro schema JSON is not an object".to_string(),
            )
        })?;
        let fields_val = obj.get_mut("fields").ok_or_else(|| {
            DataFusionError::Execution(
                "Top-level Avro schema JSON has no `fields` key".to_string(),
            )
        })?;
        let fields = fields_val.as_array_mut().ok_or_else(|| {
            DataFusionError::Execution(
                "Top-level Avro schema `fields` is not an array".to_string(),
            )
        })?;
        let mut by_name: HashMap<String, Value> = HashMap::new();
        for field in fields.iter() {
            if let Some(name) = field.get("name").and_then(|v| v.as_str()) {
                by_name.insert(name.to_string(), field.clone());
            }
        }
        let mut projected_fields = Vec::with_capacity(projected_field_names.len());
        for name in projected_field_names {
            let Some(field) = by_name.get(*name) else {
                return Err(DataFusionError::Execution(format!(
                    "Projected field `{name}` not found in Avro writer schema"
                )));
            };
            projected_fields.push(field.clone());
        }
        *fields_val = Value::Array(projected_fields);
        let projected_json = serde_json::to_string(&schema_json).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to serialize projected Avro schema JSON: {e}"
            ))
        })?;
        Ok(AvroSchema::new(projected_json))
    }
}
