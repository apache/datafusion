impl serde::Serialize for Column {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.relation.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Column", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.relation.as_ref() {
            struct_ser.serialize_field("relation", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Column {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "relation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Relation,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "relation" => Ok(GeneratedField::Relation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Column")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Column, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut relation__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Relation => {
                            if relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relation"));
                            }
                            relation__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Column {
                    name: name__.unwrap_or_default(),
                    relation: relation__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Column", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnRelation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.relation.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ColumnRelation", len)?;
        if !self.relation.is_empty() {
            struct_ser.serialize_field("relation", &self.relation)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnRelation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "relation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Relation,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "relation" => Ok(GeneratedField::Relation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnRelation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ColumnRelation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColumnRelation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut relation__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Relation => {
                            if relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relation"));
                            }
                            relation__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ColumnRelation {
                    relation: relation__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ColumnRelation", FIELDS, GeneratedVisitor)
    }
}
