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

use crate::reader::ExArrayReaderConsumer;
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;

pub trait ExInstantiable {
    type StackType<'a>;
}

pub trait ExArgType: ExInstantiable {
    fn logical_type() -> NativeType;

    fn decode(
        arg: ColumnarValue,
        consumer: impl for<'a> ExArrayReaderConsumer<ValueType<'a> = Self::StackType<'a>>,
    ) -> Result<()>;
}

pub type FindExArgType<T> = <T as ExFindImplementation>::Type;

pub trait ExFindImplementation {
    type Type: ExArgType;
}

impl<T> ExFindImplementation for T
where
    T: ExArgType,
{
    type Type = T;
}
