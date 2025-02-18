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
use crate::builder::{ExArrayBuilder, ExFullResultType};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::Result;

impl<T> ExFullResultType for ((), Option<T>)
where
    ((), T): ExFullResultType<BuilderType: ExArrayBuilder<OutArg = ()>>,
{
    type BuilderType =
        ResultBuilderWithOptionSupport<<((), T) as ExFullResultType>::BuilderType>;

    fn data_type() -> DataType {
        <((), T) as ExFullResultType>::data_type()
    }

    fn builder_with_capacity(number_rows: usize) -> Self::BuilderType {
        Self::BuilderType {
            delegate: <((), T) as ExFullResultType>::builder_with_capacity(number_rows),
        }
    }
}

pub struct ResultBuilderWithOptionSupport<Delegate> {
    delegate: Delegate,
}

impl<Delegate> ExArrayBuilder for ResultBuilderWithOptionSupport<Delegate>
where
    Delegate: ExArrayBuilder<OutArg = ()>,
{
    type OutArg = Delegate::OutArg;
    type Return = Option<Delegate::Return>;

    fn get_out_arg(
        &mut self,
        position: usize,
    ) -> <Self::OutArg as ExInstantiable>::StackType<'_> {
        self.delegate.get_out_arg(position)
    }

    fn append(&mut self, fn_ret: Self::Return) -> Result<()> {
        if let Some(ret) = fn_ret {
            self.delegate.append(ret)
        } else {
            self.delegate.append_null()
        }
    }

    fn append_null(&mut self) -> Result<()> {
        self.delegate.append_null()
    }

    fn build(self) -> Result<ArrayRef> {
        self.delegate.build()
    }
}
