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

//! Options related to how json files should be written

use std::str::FromStr;

use crate::{
    config::ConfigOptions,
    error::{DataFusionError, Result},
    parsers::CompressionTypeVariant,
};

use super::StatementOptions;

/// Options for writing JSON files
#[derive(Clone, Debug)]
pub struct JsonWriterOptions {
    pub compression: CompressionTypeVariant,
}

impl JsonWriterOptions {
    pub fn new(compression: CompressionTypeVariant) -> Self {
        Self { compression }
    }
}

impl TryFrom<(&ConfigOptions, &StatementOptions)> for JsonWriterOptions {
    type Error = DataFusionError;

    fn try_from(value: (&ConfigOptions, &StatementOptions)) -> Result<Self> {
        let _configs = value.0;
        let statement_options = value.1;
        let mut compression = CompressionTypeVariant::UNCOMPRESSED;
        for (option, value) in &statement_options.options {
            match option.to_lowercase().as_str(){
                "compression" => {
                    compression = CompressionTypeVariant::from_str(value.replace('\'', "").as_str())?;
                },
                _ => return Err(DataFusionError::Configuration(format!("Found unsupported option {option} with value {value} for JSON format!")))
            }
        }
        Ok(JsonWriterOptions { compression })
    }
}
