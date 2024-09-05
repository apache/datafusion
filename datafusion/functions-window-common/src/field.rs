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

use datafusion_common::arrow::datatypes::DataType;

pub struct FieldArgs<'a> {
    input_types: &'a [DataType],
    schema_name: &'a str,
}

impl<'a> FieldArgs<'a> {
    pub fn new(input_types: &'a [DataType], schema_name: &'a str) -> Self {
        FieldArgs {
            input_types,
            schema_name,
        }
    }

    pub fn input_types(&self) -> &[DataType] {
        self.input_types
    }

    pub fn name(&self) -> &str {
        self.schema_name
    }

    pub fn get_input_type(&self, index: usize) -> Option<DataType> {
        self.input_types.get(index).cloned()
    }
}
