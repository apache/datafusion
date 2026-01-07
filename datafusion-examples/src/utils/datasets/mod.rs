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

use std::path::PathBuf;

use arrow_schema::SchemaRef;
use datafusion::error::Result;
use datafusion_common::DataFusionError;

pub mod cars;
pub mod regex;

/// Describes example datasets used across DataFusion examples.
///
/// This enum provides a single, discoverable place to define
/// dataset-specific metadata such as file paths and schemas.
#[derive(Debug)]
pub enum ExampleDataset {
    Cars,
    Regex,
}

impl ExampleDataset {
    pub fn file_stem(&self) -> &'static str {
        match self {
            Self::Cars => "cars",
            Self::Regex => "regex",
        }
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join("csv")
            .join(format!("{}.csv", self.file_stem()))
    }

    pub fn path_str(&self) -> Result<String> {
        self.path().to_str().map(String::from).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "CSV directory path is not valid UTF-8: {}",
                self.path().display()
            ))
        })
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Cars => cars::schema(),
            Self::Regex => regex::schema(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn example_dataset_file_stem() {
        assert_eq!(ExampleDataset::Cars.file_stem(), "cars");
        assert_eq!(ExampleDataset::Regex.file_stem(), "regex");
    }

    #[test]
    fn example_dataset_path_points_to_csv() {
        let path = ExampleDataset::Cars.path();
        assert!(path.ends_with("data/csv/cars.csv"));

        let path = ExampleDataset::Regex.path();
        assert!(path.ends_with("data/csv/regex.csv"));
    }

    #[test]
    fn example_dataset_path_str_is_valid_utf8() {
        let path = ExampleDataset::Cars.path_str().unwrap();
        assert!(path.ends_with("cars.csv"));

        let path = ExampleDataset::Regex.path_str().unwrap();
        assert!(path.ends_with("regex.csv"));
    }

    #[test]
    fn cars_schema_is_stable() {
        let schema = ExampleDataset::Cars.schema();

        let fields: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone()))
            .collect();

        assert_eq!(
            fields,
            vec![
                ("car", DataType::Utf8),
                ("speed", DataType::Float64),
                ("time", DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ]
        );
    }

    #[test]
    fn regex_schema_is_stable() {
        let schema = ExampleDataset::Regex.schema();

        let fields: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone()))
            .collect();

        assert_eq!(
            fields,
            vec![
                ("values", DataType::Utf8),
                ("patterns", DataType::Utf8),
                ("replacement", DataType::Utf8),
                ("flags", DataType::Utf8),
            ]
        );
    }
}
