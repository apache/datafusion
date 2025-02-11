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

//! Contract between the macro and the library

use crate::arg_type::ExInstantiable;

pub trait ExcaliburScalarUdf {
    // for example "my_function"
    const SQL_NAME: &'static str;

    // for example
    // - (i32, (u64, ()) for my_function(a: i32, b: u64)
    // - (i32, (u64, ()) for my_function(a: i32, b: u64, out: &mut X)
    // excludes the out arg
    type ArgumentRustTypes: ExInstantiable;

    // T for `&mut T` passed to the function or () is there is no out argument
    type OutArgRustType: ExInstantiable;

    // for example i32 for my_function(..) -> i32
    type ReturnRustType;

    fn invoke(
        regular_args: <Self::ArgumentRustTypes as ExInstantiable>::StackType<'_>,
        out_arg: &mut <Self::OutArgRustType as ExInstantiable>::StackType<'_>,
    ) -> Self::ReturnRustType;
}
