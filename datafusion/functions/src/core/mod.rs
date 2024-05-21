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

//! "core" DataFusion functions

use datafusion_expr::ScalarUDF;
use std::sync::Arc;

pub mod arrow_cast;
pub mod arrowtypeof;
pub mod coalesce;
pub mod expr_ext;
pub mod getfield;
pub mod named_struct;
pub mod nullif;
pub mod nvl;
pub mod nvl2;
pub mod r#struct;

// create UDFs
make_udf_function!(arrow_cast::ArrowCastFunc, ARROW_CAST, arrow_cast);
make_udf_function!(nullif::NullIfFunc, NULLIF, nullif);
make_udf_function!(nvl::NVLFunc, NVL, nvl);
make_udf_function!(nvl2::NVL2Func, NVL2, nvl2);
make_udf_function!(arrowtypeof::ArrowTypeOfFunc, ARROWTYPEOF, arrow_typeof);
make_udf_function!(r#struct::StructFunc, STRUCT, r#struct);
make_udf_function!(named_struct::NamedStructFunc, NAMED_STRUCT, named_struct);
make_udf_function!(getfield::GetFieldFunc, GET_FIELD, get_field);
make_udf_function!(coalesce::CoalesceFunc, COALESCE, coalesce);

// Export the functions out of this package, both as expr_fn as well as a list of functions
pub mod expr_fn {
    use datafusion_expr::{Expr, Literal};

    /// returns NULL if value1 equals value2; otherwise it returns value1. This
    /// can be used to perform the inverse operation of the COALESCE expression
    pub fn nullif(arg1: Expr, arg2: Expr) -> Expr {
        super::nullif().call(vec![arg1, arg2])
    }

    /// returns value1 cast to the `arrow_type` given the second argument. This
    /// can be used to cast to a specific `arrow_type`.
    pub fn arrow_cast(arg1: Expr, arg2: Expr) -> Expr {
        super::arrow_cast().call(vec![arg1, arg2])
    }

    /// Returns value2 if value1 is NULL; otherwise it returns value1
    pub fn nvl(arg1: Expr, arg2: Expr) -> Expr {
        super::nvl().call(vec![arg1, arg2])
    }

    /// Returns value2 if value1 is not NULL; otherwise, it returns value3.
    pub fn nvl2(arg1: Expr, arg2: Expr, arg3: Expr) -> Expr {
        super::nvl2().call(vec![arg1, arg2, arg3])
    }

    /// Returns the Arrow type of the input expression.
    pub fn arrow_typeof(arg1: Expr) -> Expr {
        super::arrow_typeof().call(vec![arg1])
    }

    /// Returns a struct with the given arguments
    pub fn r#struct(args: Vec<Expr>) -> Expr {
        super::r#struct().call(args)
    }

    /// Returns a struct with the given names and arguments pairs
    pub fn named_struct(args: Vec<Expr>) -> Expr {
        super::named_struct().call(args)
    }

    /// Returns the value of the field with the given name from the struct
    pub fn get_field(arg1: Expr, field_name: impl Literal) -> Expr {
        super::get_field().call(vec![arg1, field_name.lit()])
    }

    /// Returns `coalesce(args...)`, which evaluates to the value of the first expr which is not NULL
    pub fn coalesce(args: Vec<Expr>) -> Expr {
        super::coalesce().call(args)
    }
}

///   Return a list of all functions in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        nullif(),
        arrow_cast(),
        nvl(),
        nvl2(),
        arrow_typeof(),
        r#struct(),
        named_struct(),
        get_field(),
        coalesce(),
    ]
}
