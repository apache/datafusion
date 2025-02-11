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

use arrow::datatypes::{Field, Fields, UnionFields};
use std::hash::{Hash, Hasher};
use std::{ops::Deref, sync::Arc};

use super::{LogicalTypeRef, NativeType};

/// A record of a logical type, its name and its nullability.
#[derive(Debug, Clone, Eq, PartialOrd, Ord)]
pub struct LogicalField {
    pub name: String,
    pub logical_type: LogicalTypeRef,
    pub nullable: bool,
}

impl PartialEq for LogicalField {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.logical_type.eq(&other.logical_type)
            && self.nullable == other.nullable
    }
}

impl Hash for LogicalField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.logical_type.hash(state);
        self.nullable.hash(state);
    }
}

impl From<&Field> for LogicalField {
    fn from(value: &Field) -> Self {
        Self {
            name: value.name().clone(),
            logical_type: Arc::new(NativeType::from(value.data_type().clone())),
            nullable: value.is_nullable(),
        }
    }
}

/// A reference counted [`LogicalField`].
pub type LogicalFieldRef = Arc<LogicalField>;

/// A cheaply cloneable, owned collection of [`LogicalFieldRef`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalFields(Arc<[LogicalFieldRef]>);

impl Deref for LogicalFields {
    type Target = [LogicalFieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl From<&Fields> for LogicalFields {
    fn from(value: &Fields) -> Self {
        value
            .iter()
            .map(|field| Arc::new(LogicalField::from(field.as_ref())))
            .collect()
    }
}

impl FromIterator<LogicalFieldRef> for LogicalFields {
    fn from_iter<T: IntoIterator<Item = LogicalFieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// A cheaply cloneable, owned collection of [`LogicalFieldRef`] and their
/// corresponding type ids.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalUnionFields(Arc<[(i8, LogicalFieldRef)]>);

impl Deref for LogicalUnionFields {
    type Target = [(i8, LogicalFieldRef)];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl From<&UnionFields> for LogicalUnionFields {
    fn from(value: &UnionFields) -> Self {
        value
            .iter()
            .map(|(i, field)| (i, Arc::new(LogicalField::from(field.as_ref()))))
            .collect()
    }
}

impl FromIterator<(i8, LogicalFieldRef)> for LogicalUnionFields {
    fn from_iter<T: IntoIterator<Item = (i8, LogicalFieldRef)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}
