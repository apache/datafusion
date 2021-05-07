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

use pyo3::prelude::*;

use datafusion::scalar::ScalarValue as _Scalar;

use crate::to_rust::to_rust_scalar;

/// An expression that can be used on a DataFrame
#[derive(Debug, Clone)]
pub(crate) struct Scalar {
    pub(crate) scalar: _Scalar,
}

impl<'source> FromPyObject<'source> for Scalar {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(Self {
            scalar: to_rust_scalar(ob)?,
        })
    }
}
