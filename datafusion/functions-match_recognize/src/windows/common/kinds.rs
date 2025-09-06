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

#[derive(Copy, Clone, Debug)]
pub(crate) enum MRShiftKind {
    Prev,
    Next,
}

impl MRShiftKind {
    pub(super) fn offset_sign(&self) -> i64 {
        match self {
            MRShiftKind::Prev => 1,
            MRShiftKind::Next => -1,
        }
    }

    pub(super) fn is_causal(&self) -> bool {
        matches!(self, MRShiftKind::Prev)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum MREdgeKind {
    First,
    Last,
}
