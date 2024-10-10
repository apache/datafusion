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

use core::fmt;
use std::{cmp::Ordering, hash::Hash, sync::Arc};

use super::NativeType;

/// A reference counted [`LogicalType`]
pub type LogicalTypeRef = Arc<dyn LogicalType>;

pub trait LogicalType: fmt::Debug {
    fn native(&self) -> &NativeType;
    fn name(&self) -> Option<&str>;
}

impl PartialEq for dyn LogicalType {
    fn eq(&self, other: &Self) -> bool {
        self.native().eq(other.native()) && self.name().eq(&other.name())
    }
}

impl Eq for dyn LogicalType {}

impl PartialOrd for dyn LogicalType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn LogicalType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name()
            .cmp(&other.name())
            .then(self.native().cmp(other.native()))
    }
}

impl Hash for dyn LogicalType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
        self.native().hash(state);
    }
}
