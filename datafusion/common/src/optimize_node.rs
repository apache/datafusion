
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
    pub optimzied_data: Option<T>,
    // Used to store the original data if optimized successfully
    pub original_data: T,
    pub optimized_state: OptimizedState,
    // Used to store the error if optimized failed, so we can early return but preserve the original data
    pub error: Option<E>,
}

impl<T, E> Optimized<T, E> {
    pub fn yes(optimzied_data: T, original_data: T) -> Self {
        Self {
            optimzied_data: Some(optimzied_data),
            original_data,
            optimized_state: OptimizedState::Yes,
            error: None,
        }
    }

    pub fn no(original_data: T) -> Self {
        Self {
            optimzied_data: None,
            original_data,
            optimized_state: OptimizedState::No,
            error: None,
        }
    }

    pub fn fail(original_data: T, e: E) -> Self {
        Self {
            optimzied_data: None,
            original_data,
            optimized_state: OptimizedState::Fail,
            error: Some(e),
        }
    }
}