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

use crate::arg_type::{ExArgType, ExInstantiable};
use crate::reader::{ExArrayReader, ExArrayReaderConsumer};
use datafusion_common::types::NativeType;
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use std::marker::PhantomData;

impl<T> ExInstantiable for Option<T>
where
    T: ExArgType,
{
    type StackType<'a> = Option<T::StackType<'a>>;
}

impl<T> ExArgType for Option<T>
where
    T: ExArgType,
{
    fn logical_type() -> NativeType {
        T::logical_type()
    }

    fn decode(
        arg: ColumnarValue,
        consumer: impl for<'a> ExArrayReaderConsumer<ValueType<'a> = Self::StackType<'a>>,
    ) -> Result<()> {
        let consumer = NullableConsumer {
            _t: PhantomData,
            delegate: consumer,
        };
        T::decode(arg, consumer)
    }
}

struct NullableConsumer<T, Delegate> {
    _t: PhantomData<T>,
    delegate: Delegate,
}

impl<T, Delegate> ExArrayReaderConsumer for NullableConsumer<T, Delegate>
where
    T: ExArgType,
    Delegate: for<'a> ExArrayReaderConsumer<ValueType<'a> = Option<T::StackType<'a>>>,
{
    type ValueType<'a> = T::StackType<'a>;

    fn consume<'a, AR>(self, reader: AR) -> Result<()>
    where
        AR: ExArrayReader<'a, ValueType = Self::ValueType<'a>>,
    {
        let NullableConsumer { _t: _, delegate } = self;
        let reader = NullableReader { delegate: reader };
        delegate.consume(reader)
    }
}

struct NullableReader<Delegate> {
    delegate: Delegate,
}

impl<'a, Delegate> ExArrayReader<'a> for NullableReader<Delegate>
where
    Delegate: ExArrayReader<'a>,
{
    type ValueType = Option<Delegate::ValueType>;

    fn is_valid(&self, _position: usize) -> bool {
        true
    }

    fn get(&self, position: usize) -> Self::ValueType {
        if self.delegate.is_valid(position) {
            Some(self.delegate.get(position))
        } else {
            None
        }
    }
}

// Generic reader for scalar values
impl<T> ExArrayReader<'_> for Option<T>
where
    T: Copy,
{
    type ValueType = T;

    fn is_valid(&self, _position: usize) -> bool {
        self.is_some()
    }

    fn get(&self, _position: usize) -> Self::ValueType {
        self.unwrap()
    }
}
