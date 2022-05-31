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

//! Sort functionalities

use crate::physical_plan::SendableRecordBatchStream;
use std::fmt::{Debug, Formatter};

mod cursor;
mod index;
pub mod sort;
pub mod sort_preserving_merge;

pub use cursor::SortKeyCursor;
pub use index::RowIndex;

pub(crate) struct SortedStream {
    stream: SendableRecordBatchStream,
    mem_used: usize,
}

impl Debug for SortedStream {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InMemSorterStream")
    }
}

impl SortedStream {
    pub(crate) fn new(stream: SendableRecordBatchStream, mem_used: usize) -> Self {
        Self { stream, mem_used }
    }
}
