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

//! This module contains code for reading [Avro] data into `RecordBatch`es
//!
//! [Avro]: https://avro.apache.org/docs/1.2.0/

mod arrow_array_reader;
mod reader;
mod schema;

use arrow::datatypes::Schema;
pub use reader::{Reader, ReaderBuilder};

pub use schema::to_arrow_schema;
use std::io::Read;

/// Read Avro schema given a reader
pub fn read_avro_schema_from_reader<R: Read>(
    reader: &mut R,
) -> datafusion_common::Result<Schema> {
    let avro_reader = apache_avro::Reader::new(reader)?;
    let schema = avro_reader.writer_schema();
    to_arrow_schema(schema)
}
