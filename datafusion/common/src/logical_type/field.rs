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

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_schema::{Field, FieldRef};

use super::{TypeRelation, LogicalPhysicalType};

pub type LogicalPhysicalFieldRef = Arc<LogicalPhysicalField>;

#[derive(Debug, Clone)]
pub struct LogicalPhysicalField {
    name: String,
    data_type: LogicalPhysicalType,
    nullable: bool,
    metadata: HashMap<String, String>,
}

impl From<&Field> for LogicalPhysicalField {
    fn from(value: &Field) -> Self {
        Self::new(value.name().clone(), value.data_type(), value.is_nullable())
    }
}

impl From<Field> for LogicalPhysicalField {
    fn from(value: Field) -> Self {
        Self::from(&value)
    }
}

impl From<&FieldRef> for LogicalPhysicalField {
    fn from(value: &FieldRef) -> Self {
        Self::from(value.as_ref())
    }
}

impl From<FieldRef> for LogicalPhysicalField {
    fn from(value: FieldRef) -> Self {
        Self::from(value.as_ref())
    }
}

impl Into<Field> for LogicalPhysicalField {
    fn into(self) -> Field {
        Field::new(self.name, self.data_type.physical().clone(), self.nullable)
            .with_metadata(self.metadata)
    }
}

impl PartialEq for LogicalPhysicalField {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.nullable == other.nullable
            && self.metadata == other.metadata
    }
}

impl Eq for LogicalPhysicalField {}

impl Hash for LogicalPhysicalField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);

        // ensure deterministic key order
        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(state);
            self.metadata.get(k).expect("key valid").hash(state);
        }
    }
}

impl LogicalPhysicalField {
    pub fn new(
        name: impl Into<String>,
        data_type: impl Into<LogicalPhysicalType>,
        nullable: bool,
    ) -> Self {
        LogicalPhysicalField {
            name: name.into(),
            data_type: data_type.into(),
            nullable,
            metadata: HashMap::default(),
        }
    }

    pub fn new_list_field(data_type: impl Into<LogicalPhysicalType>, nullable: bool) -> Self {
        Self::new("item", data_type, nullable)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &LogicalPhysicalType {
        &self.data_type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    #[inline]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    #[inline]
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    #[inline]
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    #[inline]
    pub fn with_data_type(mut self, data_type: LogicalPhysicalType) -> Self {
        self.data_type = data_type;
        self
    }
}

impl std::fmt::Display for LogicalPhysicalField {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
