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

use crate::__private::ExInstantiable;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::Result;

pub trait ExFullResultType {
    type BuilderType: ExArrayBuilder;

    fn data_type() -> DataType;

    fn builder_with_capacity(number_rows: usize) -> Self::BuilderType;
}

pub trait ExArrayBuilder {
    type OutArg: ExInstantiable;
    type Return;

    fn get_out_arg(
        &mut self,
        position: usize,
    ) -> <Self::OutArg as ExInstantiable>::StackType<'_>;

    fn append(&mut self, fn_ret: Self::Return) -> Result<()>;

    fn append_null(&mut self) -> Result<()>;

    fn build(self) -> Result<ArrayRef>;
}

impl ExInstantiable for () {
    type StackType<'a> = ();
}
