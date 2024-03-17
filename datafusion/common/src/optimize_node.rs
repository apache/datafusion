
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

use crate::DataFusionError;

#[derive(Debug, PartialEq, Eq)]
pub enum OptimizedState {
    Yes,
    No,
    Fail,
}

#[derive(Debug)]
pub struct Optimized<T, E = DataFusionError> {
    pub data: T,
    pub optimized_state: OptimizedState,
    pub error: Option<E>,
}

impl<T, E> Optimized<T, E> {
    /// Create a new `Transformed` object with the given information.
    pub fn new(data: T, optimized_state: OptimizedState) -> Self {
        Self {
            data,
            optimized_state,
            error: None,
        }
    }


    pub fn yes(data: T) -> Self {
        Self::new(data, OptimizedState::Yes)
    }

    pub fn no(data: T) -> Self {
        Self::new(data, OptimizedState::No)
    }

    pub fn fail(data: T, e: E) -> Self {
        Self {
            data,
            optimized_state: OptimizedState::Fail,
            error: Some(e),
        }
    }
}