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

use crate::arg_type::{ExArgType, ExFindImplementation, ExInstantiable};
use crate::builder::{ExArrayBuilder, ExFullResultType};
use crate::reader::{ExArrayReader, ExArrayReaderConsumer};
use crate::ret_type::ExFindOutImplementation;
use crate::ValuePresence;
use arrow::array::{Array, ArrayRef, StringArray, StringBuilder, StringViewArray};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_string_array, as_string_view_array};
use datafusion_common::types::NativeType;
use datafusion_common::ScalarValue;
use datafusion_common::{internal_err, Result};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

impl ExFindImplementation for dyn AsRef<str> {
    type Type = RefStrArgType;
}

pub struct RefStrArgType;

impl ExInstantiable for RefStrArgType {
    type StackType<'a> = &'a str;
}

impl ExArgType for RefStrArgType {
    fn logical_type() -> NativeType {
        NativeType::String
    }

    fn decode(
        arg: ColumnarValue,
        consumer: impl for<'a> ExArrayReaderConsumer<ValueType<'a> = Self::StackType<'a>>,
    ) -> Result<()> {
        match arg {
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8 => consumer.consume(as_string_array(&array)?),
                DataType::Utf8View => consumer.consume(as_string_view_array(&array)?),
                dt => internal_err!("Expected string array, got {:?}", dt),
            },

            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
                consumer.consume(&ScalarString(value))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8View(value)) => {
                consumer.consume(&ScalarString(value))
            }

            ColumnarValue::Scalar(scalar) => {
                internal_err!("Expected string scalar, got {:?}", scalar)
            }
        }
    }
}

impl<'a> ExArrayReader<'a> for &'a StringArray {
    type ValueType = &'a str;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(&self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        self.value(position)
    }
}

impl<'a> ExArrayReader<'a> for &'a StringViewArray {
    type ValueType = &'a str;

    fn is_valid(&self, position: usize) -> bool {
        Array::is_valid(&self, position)
    }

    fn get(&self, position: usize) -> Self::ValueType {
        self.value(position)
    }
}

struct ScalarString(Option<String>);

impl<'a> ExArrayReader<'a> for &'a ScalarString {
    type ValueType = &'a str;

    fn is_valid(&self, _position: usize) -> bool {
        self.0.is_some()
    }

    fn get(&self, _position: usize) -> Self::ValueType {
        self.0.as_deref().unwrap()
    }
}

impl ExFindOutImplementation for dyn std::fmt::Write {
    type Type = StringWriter;
}

pub struct StringWriter;

impl ExFullResultType for (StringWriter, Result<ValuePresence>) {
    type BuilderType = StringBuilder;

    fn data_type() -> DataType {
        DataType::Utf8
    }

    fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
        StringBuilder::with_capacity(number_rows, number_rows * 10)
    }
}

impl ExInstantiable for StringWriter {
    type StackType<'a> = StringBuilderWriter<'a>;
}

pub struct StringBuilderWriter<'a> {
    builder: &'a mut StringBuilder,
}

impl std::fmt::Write for StringBuilderWriter<'_> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.builder.write_str(s).map_err(|_| std::fmt::Error)
    }
}

impl ExArrayBuilder for StringBuilder {
    type OutArg = StringWriter;
    type Return = Result<ValuePresence>;

    fn get_out_arg(
        &mut self,
        _position: usize,
    ) -> <Self::OutArg as ExInstantiable>::StackType<'_> {
        StringBuilderWriter { builder: self }
    }

    fn append(&mut self, fn_ret: Self::Return) -> Result<()> {
        match fn_ret? {
            ValuePresence::Value => {
                // Data passed via the out arg
                self.append_value("");
            }
            ValuePresence::Null => {
                self.append_null();
            }
        }
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
