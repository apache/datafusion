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

use std::ops::Deref;
use std::sync::Arc;

use arrow_schema::{Field, FieldRef, Fields, UnionFields};

use super::field::{LogicalPhysicalField, LogicalPhysicalFieldRef};

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct LogicalPhysicalFields(Arc<[LogicalPhysicalFieldRef]>);

impl std::fmt::Debug for LogicalPhysicalFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl From<&Fields> for LogicalPhysicalFields {
    fn from(value: &Fields) -> Self {
        Self(
            value
                .iter()
                .map(|v| LogicalPhysicalFieldRef::new(v.into()))
                .collect(),
        )
    }
}

impl From<Fields> for LogicalPhysicalFields {
    fn from(value: Fields) -> Self {
        Self::from(&value)
    }
}

impl Into<Fields> for LogicalPhysicalFields {
    fn into(self) -> Fields {
        Fields::from(
            self.iter()
                .map(|f| f.as_ref().clone().into())
                .collect::<Vec<Field>>(),
        )
    }
}

impl Default for LogicalPhysicalFields {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromIterator<LogicalPhysicalField> for LogicalPhysicalFields {
    fn from_iter<T: IntoIterator<Item =LogicalPhysicalField>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<LogicalPhysicalFieldRef> for LogicalPhysicalFields {
    fn from_iter<T: IntoIterator<Item =LogicalPhysicalFieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<LogicalPhysicalField>> for LogicalPhysicalFields {
    fn from(value: Vec<LogicalPhysicalField>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<LogicalPhysicalFieldRef>> for LogicalPhysicalFields {
    fn from(value: Vec<LogicalPhysicalFieldRef>) -> Self {
        Self(value.into())
    }
}

impl From<&[LogicalPhysicalFieldRef]> for LogicalPhysicalFields {
    fn from(value: &[LogicalPhysicalFieldRef]) -> Self {
        Self(value.into())
    }
}

impl<const N: usize> From<[LogicalPhysicalFieldRef; N]> for LogicalPhysicalFields {
    fn from(value: [LogicalPhysicalFieldRef; N]) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for LogicalPhysicalFields {
    type Target = [LogicalPhysicalFieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a LogicalPhysicalFields {
    type Item = &'a LogicalPhysicalFieldRef;
    type IntoIter = std::slice::Iter<'a, LogicalPhysicalFieldRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl LogicalPhysicalFields {
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct LogicalUnionFields(Arc<[(i8, LogicalPhysicalFieldRef)]>);

impl std::fmt::Debug for LogicalUnionFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl FromIterator<(i8, LogicalPhysicalFieldRef)> for LogicalUnionFields {
    fn from_iter<T: IntoIterator<Item = (i8, LogicalPhysicalFieldRef)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<&UnionFields> for LogicalUnionFields {
    fn from(value: &UnionFields) -> Self {
        Self::from_iter(
            value
                .iter()
                .map(|(i, f)| (i, LogicalPhysicalFieldRef::new(f.into()))),
        )
    }
}

impl From<UnionFields> for LogicalUnionFields {
    fn from(value: UnionFields) -> Self {
        Self::from(&value)
    }
}

impl Into<UnionFields> for LogicalUnionFields {
    fn into(self) -> UnionFields {
        UnionFields::from_iter(
            self.0
                .into_iter()
                .map(|(i, f)| (*i, FieldRef::new(f.as_ref().clone().into()))),
        )
    }
}
