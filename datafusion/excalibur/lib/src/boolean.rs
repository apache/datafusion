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

use crate::builder::{ExArrayBuilder, ExFullResultType};
use crate::primitive_type;
use crate::reader::ExArrayReader;
use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::Result;
use std::sync::Arc;

primitive_type!(bool, Boolean, BooleanArray, as_boolean_array);

impl<'a> ExArrayReader<'a> for &'a BooleanArray {
    type ValueType = bool;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        self.value(position)
    }
}

impl ExFullResultType for ((), bool) {
    type BuilderType = BooleanBuilder;

    fn data_type() -> DataType {
        DataType::Boolean
    }

    fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
        Self::BuilderType::with_capacity(number_rows)
    }
}

impl ExArrayBuilder for BooleanBuilder {
    type OutArg = ();
    type Return = bool;

    fn get_out_arg(&mut self, _position: usize) -> Self::OutArg {}

    fn append(&mut self, fn_ret: Self::Return) -> Result<()> {
        self.append_value(fn_ret);
        Ok(())
    }

    fn append_null(&mut self) -> Result<()> {
        self.append_null();
        Ok(())
    }

    fn build(mut self) -> Result<ArrayRef> {
        Ok(Arc::new(self.finish()))
    }
}
