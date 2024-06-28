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

use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypeSignature {
    // **func_name**(p1, p2)
    name: Arc<str>,
    // func_name(**p1**, **p2**)
    params: Vec<Arc<str>>,
}

impl TypeSignature {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self::new_with_params(name, vec![])
    }

    pub fn new_with_params(
        name: impl Into<Arc<str>>,
        params: Vec<Arc<str>>,
    ) -> Self {
        Self {
            name: name.into(),
            params,
        }
    }
}
