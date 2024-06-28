use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use arrow_schema::{DataType, Field};
use crate::logical_type::extension::ExtensionType;
use crate::logical_type::fields::LogicalFields;
use crate::logical_type::LogicalType;
use crate::logical_type::type_signature::TypeSignature;

pub type LogicalFieldRef = Arc<LogicalField>;

#[derive(Debug, Clone)]
pub struct LogicalField {
    name: String,
    data_type: LogicalType,
    nullable: bool,
    metadata: HashMap<String, String>,
}

impl From<&Field> for LogicalField {
    fn from(value: &Field) -> Self {
        Self {
            name: value.name().clone(),
            data_type: value.data_type().clone().into(),
            nullable: value.is_nullable(),
            metadata: value.metadata().clone()
        }
    }
}

impl From<Field> for LogicalField {
    fn from(value: Field) -> Self {
        Self::from(&value)
    }
}

impl Into<Field> for LogicalField {
    fn into(self) -> Field {
       Field::new(self.name, self.data_type.physical_type(), self.nullable).with_metadata(self.metadata)
    }
}

impl PartialEq for LogicalField {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.nullable == other.nullable
            && self.metadata == other.metadata
    }
}

impl Eq for LogicalField {}

impl Hash for LogicalField {
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

impl ExtensionType for LogicalField {
    fn display_name(&self) -> &str {
       &self.name
    }

    fn type_signature(&self) -> TypeSignature {
        TypeSignature::new(self.name())
    }

    fn physical_type(&self) -> DataType {
        self.data_type.physical_type()
    }

    fn is_comparable(&self) -> bool {
        self.data_type.is_comparable()
    }

    fn is_orderable(&self) -> bool {
        self.data_type.is_orderable()
    }

    fn is_numeric(&self) -> bool {
        self.data_type.is_numeric()
    }

    fn is_floating(&self) -> bool {
       self.data_type.is_floating()
    }
}

impl LogicalField {
    pub fn new(name: impl Into<String>, data_type: LogicalType, nullable: bool) -> Self {
        LogicalField {
            name: name.into(),
            data_type,
            nullable,
            metadata: HashMap::default(),
        }
    }

    pub fn new_list_field(data_type: LogicalType, nullable: bool) -> Self {
        Self::new("item", data_type, nullable)
    }

    pub fn new_struct(name: impl Into<String>, fields: impl Into<LogicalFields>, nullable: bool) -> Self {
        Self::new(name, LogicalType::Struct(fields.into()), nullable)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &LogicalType {
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
    pub fn with_data_type(mut self, data_type: LogicalType) -> Self {
        self.data_type = data_type;
        self
    }
}

impl std::fmt::Display for LogicalField {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
