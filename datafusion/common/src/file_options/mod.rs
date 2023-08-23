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

//! Options related to how files should be written

pub mod arrow_writer;
pub mod avro_writer;
pub mod csv_writer;
pub mod file_type;
pub mod json_writer;
pub mod parquet_writer;
pub(crate) mod parse_utils;

use std::{collections::HashMap, path::Path, str::FromStr};

use crate::{
    config::ConfigOptions, file_options::parse_utils::parse_boolean_string,
    DataFusionError, FileType, Result,
};

use self::{
    arrow_writer::ArrowWriterOptions, avro_writer::AvroWriterOptions,
    csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions,
    parquet_writer::ParquetWriterOptions,
};

/// Represents a single arbitrary setting in a
/// [StatementOptions] where OptionTuple.0 determines
/// the specific setting to be modified and OptionTuple.1
/// determines the value which should be applied
pub type OptionTuple = (String, String);

/// Represents arbitrary tuples of options passed as String
/// tuples from SQL statements. As in the following statement:
/// COPY ... TO ... (setting1 value1, setting2 value2, ...)
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StatementOptions {
    options: Vec<OptionTuple>,
}

/// Useful for conversion from external tables which use Hashmap<String, String>
impl From<&HashMap<String, String>> for StatementOptions {
    fn from(value: &HashMap<String, String>) -> Self {
        Self {
            options: value
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect::<Vec<OptionTuple>>(),
        }
    }
}

impl StatementOptions {
    pub fn new(options: Vec<OptionTuple>) -> Self {
        Self { options }
    }

    pub fn into_inner(self) -> Vec<OptionTuple> {
        self.options
    }

    /// Scans for option and if it exists removes it and attempts to parse as a boolean
    /// Returns none if it does not exist.
    pub fn get_bool_option(&mut self, find: &str) -> Result<Option<bool>> {
        let maybe_option = self.scan_and_remove_option(find);
        maybe_option
            .map(|(_, v)| parse_boolean_string(find, v))
            .transpose()
    }

    /// Scans for option and if it exists removes it and returns it
    /// Returns none if it does not exist
    pub fn get_str_option(&mut self, find: &str) -> Option<String> {
        let maybe_option = self.scan_and_remove_option(find);
        maybe_option.map(|(_, v)| v)
    }

    /// Infers the file_type given a target and arbitrary options.
    /// If the options contain an explicit "format" option, that will be used.
    /// Otherwise, attempt to infer file_type from the extension of target.
    /// Finally, return an error if unable to determine the file_type
    /// If found, format is removed from the options list.
    pub fn try_infer_file_type(&mut self, target: &str) -> Result<FileType> {
        let explicit_format = self.scan_and_remove_option("format");
        let format = match explicit_format {
            Some(s) => FileType::from_str(s.1.as_str()),
            None => {
                // try to infer file format from file extension
                let extension: &str = &Path::new(target)
                    .extension()
                    .ok_or(DataFusionError::InvalidOption(
                        "Format not explicitly set and unable to get file extension!"
                            .to_string(),
                    ))?
                    .to_str()
                    .ok_or(DataFusionError::InvalidOption(
                        "Format not explicitly set and failed to parse file extension!"
                            .to_string(),
                    ))?
                    .to_lowercase();

                FileType::from_str(extension)
            }
        }?;

        Ok(format)
    }

    /// Finds an option in StatementOptions if exists, removes and returns it
    /// along with the vec of remaining options.
    fn scan_and_remove_option(&mut self, find: &str) -> Option<OptionTuple> {
        let idx = self
            .options
            .iter()
            .position(|(k, _)| k.to_lowercase() == find.to_lowercase());
        match idx {
            Some(i) => Some(self.options.swap_remove(i)),
            None => None,
        }
    }
}

/// This type contains all options needed to initialize a particular
/// RecordBatchWriter type. Each element in the enum contains a thin wrapper
/// around a "writer builder" type (e.g. arrow::csv::WriterBuilder)
/// plus any DataFusion specific writing options (e.g. CSV compression)
#[derive(Clone, Debug)]
pub enum FileTypeWriterOptions {
    Parquet(ParquetWriterOptions),
    CSV(CsvWriterOptions),
    JSON(JsonWriterOptions),
    Avro(AvroWriterOptions),
    Arrow(ArrowWriterOptions),
}

impl FileTypeWriterOptions {
    /// Constructs a FileTypeWriterOptions given a FileType to be written
    /// and arbitrary String tuple options. May return an error if any
    /// string setting is unrecognized or unsupported.
    pub fn build(
        file_type: &FileType,
        config_defaults: &ConfigOptions,
        statement_options: &StatementOptions,
    ) -> Result<Self> {
        let options = (config_defaults, statement_options);

        let file_type_write_options = match file_type {
            FileType::PARQUET => {
                FileTypeWriterOptions::Parquet(ParquetWriterOptions::try_from(options)?)
            }
            FileType::CSV => {
                FileTypeWriterOptions::CSV(CsvWriterOptions::try_from(options)?)
            }
            FileType::JSON => {
                FileTypeWriterOptions::JSON(JsonWriterOptions::try_from(options)?)
            }
            FileType::AVRO => {
                FileTypeWriterOptions::Avro(AvroWriterOptions::try_from(options)?)
            }
            FileType::ARROW => {
                FileTypeWriterOptions::Arrow(ArrowWriterOptions::try_from(options)?)
            }
        };

        Ok(file_type_write_options)
    }

    /// Constructs a FileTypeWriterOptions from session defaults only.
    pub fn build_default(
        file_type: &FileType,
        config_defaults: &ConfigOptions,
    ) -> Result<Self> {
        let empty_statement = StatementOptions::new(vec![]);
        let options = (config_defaults, &empty_statement);

        let file_type_write_options = match file_type {
            FileType::PARQUET => {
                FileTypeWriterOptions::Parquet(ParquetWriterOptions::try_from(options)?)
            }
            FileType::CSV => {
                FileTypeWriterOptions::CSV(CsvWriterOptions::try_from(options)?)
            }
            FileType::JSON => {
                FileTypeWriterOptions::JSON(JsonWriterOptions::try_from(options)?)
            }
            FileType::AVRO => {
                FileTypeWriterOptions::Avro(AvroWriterOptions::try_from(options)?)
            }
            FileType::ARROW => {
                FileTypeWriterOptions::Arrow(ArrowWriterOptions::try_from(options)?)
            }
        };

        Ok(file_type_write_options)
    }

    /// Tries to extract ParquetWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from parquet is set.
    pub fn try_into_parquet(&self) -> Result<&ParquetWriterOptions> {
        match self {
            FileTypeWriterOptions::Parquet(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(
                "Expected parquet options but found options for a different FileType!"
                    .into(),
            )),
        }
    }

    /// Tries to extract CsvWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from csv is set.
    pub fn try_into_csv(&self) -> Result<&CsvWriterOptions> {
        match self {
            FileTypeWriterOptions::CSV(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(
                "Expected csv options but found options for a different FileType!".into(),
            )),
        }
    }

    /// Tries to extract JsonWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from json is set.
    pub fn try_into_json(&self) -> Result<&JsonWriterOptions> {
        match self {
            FileTypeWriterOptions::JSON(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(
                "Expected json options but found options for a different FileType!"
                    .into(),
            )),
        }
    }

    /// Tries to extract AvroWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from avro is set.
    pub fn try_into_avro(&self) -> Result<&AvroWriterOptions> {
        match self {
            FileTypeWriterOptions::Avro(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(
                "Expected avro options but found options for a different FileType!"
                    .into(),
            )),
        }
    }

    /// Tries to extract ArrowWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from arrow is set.
    pub fn try_into_arrow(&self) -> Result<&ArrowWriterOptions> {
        match self {
            FileTypeWriterOptions::Arrow(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(
                "Expected arrow options but found options for a different FileType!"
                    .into(),
            )),
        }
    }
}
