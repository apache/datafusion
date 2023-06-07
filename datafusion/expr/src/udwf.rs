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

//! Support for user-defined window (UDWF) window functions

use std::fmt::{self, Debug, Formatter};

use crate::{ReturnTypeFunction, Signature};

/// Logical representation of a user-defined window function (UDWF)
/// A UDAF is different from a UDF in that it is stateful across batches.
#[derive(Clone)]
pub struct WindowUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    // /// actual implementation
    // pub accumulator: AccumulatorFunctionImplementation,
}

impl Debug for WindowUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("WindowUDF").finish_non_exhaustive()
    }
}

impl PartialEq for WindowUDF {
    fn eq(&self, other: &Self) -> bool {
        todo!();
        //self.name == other.name && self.signature == other.signature
    }
}

impl Eq for WindowUDF {}

impl std::hash::Hash for WindowUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // self.name.hash(state);
        // self.signature.hash(state);
    }
}

impl WindowUDF {
    // /// Create a new WindowUDF
    // pub fn new(
    //     name: &str,
    //     signature: &Signature,
    //     return_type: &ReturnTypeFunction,
    //     accumulator: &AccumulatorFunctionImplementation,
    //     state_type: &StateTypeFunction,
    // ) -> Self {
    //     Self {
    //         name: name.to_owned(),
    //         signature: signature.clone(),
    //         return_type: return_type.clone(),
    //         accumulator: accumulator.clone(),
    //         state_type: state_type.clone(),
    //     }
    // }
}
