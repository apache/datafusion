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
use crate::bridge::ExcaliburScalarUdf;
use crate::builder::{ExArrayBuilder, ExFullResultType};
use crate::reader::{ExArrayReader, ExArrayReaderConsumer};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use datafusion_expr::ScalarFunctionArgs;
use std::collections::VecDeque;

pub fn excalibur_invoke<T>(args: ScalarFunctionArgs) -> Result<ColumnarValue>
where
    T: ExcaliburScalarUdf,
    T::ArgumentRustTypes: ApplyList,
    (T::OutArgRustType, T::ReturnRustType): ExFullResultType<
        BuilderType: ExArrayBuilder<
            OutArg: for<'a> ExInstantiable<
                StackType<'a> = <T::OutArgRustType as ExInstantiable>::StackType<'a>,
            >,
            Return = T::ReturnRustType,
        >,
    >,
{
    let number_rows = args.number_rows;
    let args = args.args;
    assert_eq!(args.len(), T::ArgumentRustTypes::ARITY);
    let args = VecDeque::from(args);

    let mut builder =
        <(T::OutArgRustType, T::ReturnRustType) as ExFullResultType>::builder_with_capacity(
            number_rows,
        );

    T::ArgumentRustTypes::apply(
        args,
        number_rows,
        // valid
        |_position: usize| true,
        // apply
        |_position, args, out_arg| T::invoke(args, out_arg),
        &mut builder,
    )?;

    let array = builder.build()?;
    Ok(ColumnarValue::Array(array))
}

pub trait ApplyList: ExInstantiable {
    const ARITY: usize;

    fn apply<Builder, Valid, Invoke>(
        args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: for<'a> Fn(
            usize,
            Self::StackType<'a>,
            &mut <Builder::OutArg as ExInstantiable>::StackType<'_>,
        ) -> Builder::Return;
}

impl<Head, Tail> ExInstantiable for (Head, Tail)
where
    Head: ExArgType,
    Tail: ApplyList,
{
    type StackType<'a> = (Head::StackType<'a>, Tail::StackType<'a>);
}

impl<Head, Tail> ApplyList for (Head, Tail)
where
    Head: ExArgType,
    Tail: ApplyList,
{
    const ARITY: usize = 1 + Tail::ARITY;

    fn apply<Builder, Valid, Invoke>(
        mut args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: for<'a> Fn(
            usize,
            Self::StackType<'a>,
            &mut <Builder::OutArg as ExInstantiable>::StackType<'_>,
        ) -> Builder::Return,
    {
        let arg = args.pop_front().unwrap();
        let continuation = ApplyListHeadConsumer {
            remaining_args: args,
            number_rows,
            valid,
            invoke,
            builder,
            _phantom_head: std::marker::PhantomData::<Head>,
            _phantom_tail: std::marker::PhantomData::<Tail>,
        };
        Head::decode(arg, continuation)
    }
}

struct ApplyListHeadConsumer<'a, Head, Tail, Builder, Valid, Invoke> {
    remaining_args: VecDeque<ColumnarValue>,
    number_rows: usize,
    valid: Valid,
    invoke: Invoke,
    builder: &'a mut Builder,
    _phantom_head: std::marker::PhantomData<Head>,
    _phantom_tail: std::marker::PhantomData<Tail>,
}

impl<Head, Tail, Builder, Valid, Invoke> ExArrayReaderConsumer
    for ApplyListHeadConsumer<'_, Head, Tail, Builder, Valid, Invoke>
where
    Head: ExArgType,
    Tail: ApplyList,
    Builder: ExArrayBuilder,
    Valid: Fn(usize) -> bool,
    Invoke: for<'a> Fn(
        usize,
        (Head::StackType<'a>, Tail::StackType<'a>),
        &mut <Builder::OutArg as ExInstantiable>::StackType<'_>,
    ) -> Builder::Return,
{
    type ValueType<'a> = Head::StackType<'a>;

    fn consume<'a, AR>(self, reader: AR) -> Result<()>
    where
        AR: ExArrayReader<'a, ValueType = Self::ValueType<'a>>,
    {
        let ApplyListHeadConsumer {
            remaining_args: args,
            number_rows,
            valid,
            invoke,
            builder,
            _phantom_head,
            _phantom_tail,
        } = self;
        Tail::apply(
            args,
            number_rows,
            |position| valid(position) && reader.is_valid(position),
            |position, tail_args, out_arg| {
                let head_arg: Head::StackType<'_> = reader.get(position);
                let record = (head_arg, tail_args);
                // FIXME: here we succumb to the borrow checker
                // SAFETY: the Invoke  is guaranteed not to capture the reference it is given
                let record = unsafe {
                    std::mem::transmute::<
                        (Head::StackType<'_>, Tail::StackType<'_>),
                        (Head::StackType<'_>, Tail::StackType<'_>),
                    >(record)
                };
                invoke(position, record, out_arg)
            },
            builder,
        )
    }
}

impl ApplyList for () {
    const ARITY: usize = 0;

    fn apply<Builder, Valid, Invoke>(
        args: VecDeque<ColumnarValue>,
        number_rows: usize,
        valid: Valid,
        invoke: Invoke,
        builder: &mut Builder,
    ) -> Result<()>
    where
        Builder: ExArrayBuilder,
        Valid: Fn(usize) -> bool,
        Invoke: for<'a> Fn(
            usize,
            Self::StackType<'a>,
            &mut <Builder::OutArg as ExInstantiable>::StackType<'_>,
        ) -> Builder::Return,
    {
        assert!(args.is_empty());
        for position in 0..number_rows {
            if valid(position) {
                let mut out_arg: <Builder::OutArg as ExInstantiable>::StackType<'_> =
                    builder.get_out_arg(position);
                let result = invoke(position, (), &mut out_arg);
                drop(out_arg);
                builder.append(result)?;
            } else {
                builder.append_null()?;
            }
        }
        Ok(())
    }
}
