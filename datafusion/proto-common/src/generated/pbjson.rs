impl serde::Serialize for ArrowType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.arrow_type_enum.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ArrowType", len)?;
        if let Some(v) = self.arrow_type_enum.as_ref() {
            match v {
                arrow_type::ArrowTypeEnum::None(v) => {
                    struct_ser.serialize_field("NONE", v)?;
                }
                arrow_type::ArrowTypeEnum::Bool(v) => {
                    struct_ser.serialize_field("BOOL", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint8(v) => {
                    struct_ser.serialize_field("UINT8", v)?;
                }
                arrow_type::ArrowTypeEnum::Int8(v) => {
                    struct_ser.serialize_field("INT8", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint16(v) => {
                    struct_ser.serialize_field("UINT16", v)?;
                }
                arrow_type::ArrowTypeEnum::Int16(v) => {
                    struct_ser.serialize_field("INT16", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint32(v) => {
                    struct_ser.serialize_field("UINT32", v)?;
                }
                arrow_type::ArrowTypeEnum::Int32(v) => {
                    struct_ser.serialize_field("INT32", v)?;
                }
                arrow_type::ArrowTypeEnum::Uint64(v) => {
                    struct_ser.serialize_field("UINT64", v)?;
                }
                arrow_type::ArrowTypeEnum::Int64(v) => {
                    struct_ser.serialize_field("INT64", v)?;
                }
                arrow_type::ArrowTypeEnum::Float16(v) => {
                    struct_ser.serialize_field("FLOAT16", v)?;
                }
                arrow_type::ArrowTypeEnum::Float32(v) => {
                    struct_ser.serialize_field("FLOAT32", v)?;
                }
                arrow_type::ArrowTypeEnum::Float64(v) => {
                    struct_ser.serialize_field("FLOAT64", v)?;
                }
                arrow_type::ArrowTypeEnum::Utf8(v) => {
                    struct_ser.serialize_field("UTF8", v)?;
                }
                arrow_type::ArrowTypeEnum::LargeUtf8(v) => {
                    struct_ser.serialize_field("LARGEUTF8", v)?;
                }
                arrow_type::ArrowTypeEnum::Binary(v) => {
                    struct_ser.serialize_field("BINARY", v)?;
                }
                arrow_type::ArrowTypeEnum::FixedSizeBinary(v) => {
                    struct_ser.serialize_field("FIXEDSIZEBINARY", v)?;
                }
                arrow_type::ArrowTypeEnum::LargeBinary(v) => {
                    struct_ser.serialize_field("LARGEBINARY", v)?;
                }
                arrow_type::ArrowTypeEnum::Date32(v) => {
                    struct_ser.serialize_field("DATE32", v)?;
                }
                arrow_type::ArrowTypeEnum::Date64(v) => {
                    struct_ser.serialize_field("DATE64", v)?;
                }
                arrow_type::ArrowTypeEnum::Duration(v) => {
                    let v = TimeUnit::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("DURATION", &v)?;
                }
                arrow_type::ArrowTypeEnum::Timestamp(v) => {
                    struct_ser.serialize_field("TIMESTAMP", v)?;
                }
                arrow_type::ArrowTypeEnum::Time32(v) => {
                    let v = TimeUnit::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("TIME32", &v)?;
                }
                arrow_type::ArrowTypeEnum::Time64(v) => {
                    let v = TimeUnit::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("TIME64", &v)?;
                }
                arrow_type::ArrowTypeEnum::Interval(v) => {
                    let v = IntervalUnit::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("INTERVAL", &v)?;
                }
                arrow_type::ArrowTypeEnum::Decimal(v) => {
                    struct_ser.serialize_field("DECIMAL", v)?;
                }
                arrow_type::ArrowTypeEnum::List(v) => {
                    struct_ser.serialize_field("LIST", v)?;
                }
                arrow_type::ArrowTypeEnum::LargeList(v) => {
                    struct_ser.serialize_field("LARGELIST", v)?;
                }
                arrow_type::ArrowTypeEnum::FixedSizeList(v) => {
                    struct_ser.serialize_field("FIXEDSIZELIST", v)?;
                }
                arrow_type::ArrowTypeEnum::Struct(v) => {
                    struct_ser.serialize_field("STRUCT", v)?;
                }
                arrow_type::ArrowTypeEnum::Union(v) => {
                    struct_ser.serialize_field("UNION", v)?;
                }
                arrow_type::ArrowTypeEnum::Dictionary(v) => {
                    struct_ser.serialize_field("DICTIONARY", v)?;
                }
                arrow_type::ArrowTypeEnum::Map(v) => {
                    struct_ser.serialize_field("MAP", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ArrowType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NONE",
            "BOOL",
            "UINT8",
            "INT8",
            "UINT16",
            "INT16",
            "UINT32",
            "INT32",
            "UINT64",
            "INT64",
            "FLOAT16",
            "FLOAT32",
            "FLOAT64",
            "UTF8",
            "LARGE_UTF8",
            "LARGEUTF8",
            "BINARY",
            "FIXED_SIZE_BINARY",
            "FIXEDSIZEBINARY",
            "LARGE_BINARY",
            "LARGEBINARY",
            "DATE32",
            "DATE64",
            "DURATION",
            "TIMESTAMP",
            "TIME32",
            "TIME64",
            "INTERVAL",
            "DECIMAL",
            "LIST",
            "LARGE_LIST",
            "LARGELIST",
            "FIXED_SIZE_LIST",
            "FIXEDSIZELIST",
            "STRUCT",
            "UNION",
            "DICTIONARY",
            "MAP",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            None,
            Bool,
            Uint8,
            Int8,
            Uint16,
            Int16,
            Uint32,
            Int32,
            Uint64,
            Int64,
            Float16,
            Float32,
            Float64,
            Utf8,
            LargeUtf8,
            Binary,
            FixedSizeBinary,
            LargeBinary,
            Date32,
            Date64,
            Duration,
            Timestamp,
            Time32,
            Time64,
            Interval,
            Decimal,
            List,
            LargeList,
            FixedSizeList,
            Struct,
            Union,
            Dictionary,
            Map,
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
                            "NONE" => Ok(GeneratedField::None),
                            "BOOL" => Ok(GeneratedField::Bool),
                            "UINT8" => Ok(GeneratedField::Uint8),
                            "INT8" => Ok(GeneratedField::Int8),
                            "UINT16" => Ok(GeneratedField::Uint16),
                            "INT16" => Ok(GeneratedField::Int16),
                            "UINT32" => Ok(GeneratedField::Uint32),
                            "INT32" => Ok(GeneratedField::Int32),
                            "UINT64" => Ok(GeneratedField::Uint64),
                            "INT64" => Ok(GeneratedField::Int64),
                            "FLOAT16" => Ok(GeneratedField::Float16),
                            "FLOAT32" => Ok(GeneratedField::Float32),
                            "FLOAT64" => Ok(GeneratedField::Float64),
                            "UTF8" => Ok(GeneratedField::Utf8),
                            "LARGEUTF8" | "LARGE_UTF8" => Ok(GeneratedField::LargeUtf8),
                            "BINARY" => Ok(GeneratedField::Binary),
                            "FIXEDSIZEBINARY" | "FIXED_SIZE_BINARY" => Ok(GeneratedField::FixedSizeBinary),
                            "LARGEBINARY" | "LARGE_BINARY" => Ok(GeneratedField::LargeBinary),
                            "DATE32" => Ok(GeneratedField::Date32),
                            "DATE64" => Ok(GeneratedField::Date64),
                            "DURATION" => Ok(GeneratedField::Duration),
                            "TIMESTAMP" => Ok(GeneratedField::Timestamp),
                            "TIME32" => Ok(GeneratedField::Time32),
                            "TIME64" => Ok(GeneratedField::Time64),
                            "INTERVAL" => Ok(GeneratedField::Interval),
                            "DECIMAL" => Ok(GeneratedField::Decimal),
                            "LIST" => Ok(GeneratedField::List),
                            "LARGELIST" | "LARGE_LIST" => Ok(GeneratedField::LargeList),
                            "FIXEDSIZELIST" | "FIXED_SIZE_LIST" => Ok(GeneratedField::FixedSizeList),
                            "STRUCT" => Ok(GeneratedField::Struct),
                            "UNION" => Ok(GeneratedField::Union),
                            "DICTIONARY" => Ok(GeneratedField::Dictionary),
                            "MAP" => Ok(GeneratedField::Map),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ArrowType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ArrowType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ArrowType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut arrow_type_enum__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::None => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("NONE"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::None)
;
                        }
                        GeneratedField::Bool => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("BOOL"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Bool)
;
                        }
                        GeneratedField::Uint8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT8"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint8)
;
                        }
                        GeneratedField::Int8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT8"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int8)
;
                        }
                        GeneratedField::Uint16 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT16"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint16)
;
                        }
                        GeneratedField::Int16 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT16"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int16)
;
                        }
                        GeneratedField::Uint32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT32"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint32)
;
                        }
                        GeneratedField::Int32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT32"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int32)
;
                        }
                        GeneratedField::Uint64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UINT64"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Uint64)
;
                        }
                        GeneratedField::Int64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INT64"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Int64)
;
                        }
                        GeneratedField::Float16 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FLOAT16"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Float16)
;
                        }
                        GeneratedField::Float32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FLOAT32"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Float32)
;
                        }
                        GeneratedField::Float64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FLOAT64"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Float64)
;
                        }
                        GeneratedField::Utf8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UTF8"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Utf8)
;
                        }
                        GeneratedField::LargeUtf8 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LARGEUTF8"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::LargeUtf8)
;
                        }
                        GeneratedField::Binary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("BINARY"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Binary)
;
                        }
                        GeneratedField::FixedSizeBinary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FIXEDSIZEBINARY"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| arrow_type::ArrowTypeEnum::FixedSizeBinary(x.0));
                        }
                        GeneratedField::LargeBinary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LARGEBINARY"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::LargeBinary)
;
                        }
                        GeneratedField::Date32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DATE32"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Date32)
;
                        }
                        GeneratedField::Date64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DATE64"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Date64)
;
                        }
                        GeneratedField::Duration => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DURATION"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<TimeUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Duration(x as i32));
                        }
                        GeneratedField::Timestamp => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("TIMESTAMP"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Timestamp)
;
                        }
                        GeneratedField::Time32 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("TIME32"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<TimeUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Time32(x as i32));
                        }
                        GeneratedField::Time64 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("TIME64"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<TimeUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Time64(x as i32));
                        }
                        GeneratedField::Interval => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("INTERVAL"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<IntervalUnit>>()?.map(|x| arrow_type::ArrowTypeEnum::Interval(x as i32));
                        }
                        GeneratedField::Decimal => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DECIMAL"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Decimal)
;
                        }
                        GeneratedField::List => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LIST"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::List)
;
                        }
                        GeneratedField::LargeList => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("LARGELIST"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::LargeList)
;
                        }
                        GeneratedField::FixedSizeList => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FIXEDSIZELIST"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::FixedSizeList)
;
                        }
                        GeneratedField::Struct => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("STRUCT"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Struct)
;
                        }
                        GeneratedField::Union => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UNION"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Union)
;
                        }
                        GeneratedField::Dictionary => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DICTIONARY"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Dictionary)
;
                        }
                        GeneratedField::Map => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("MAP"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Map)
;
                        }
                    }
                }
                Ok(ArrowType {
                    arrow_type_enum: arrow_type_enum__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ArrowType", FIELDS, GeneratedVisitor)
    }
}
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
impl serde::Serialize for Decimal {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.precision != 0 {
            len += 1;
        }
        if self.scale != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Decimal", len)?;
        if self.precision != 0 {
            struct_ser.serialize_field("precision", &self.precision)?;
        }
        if self.scale != 0 {
            struct_ser.serialize_field("scale", &self.scale)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Decimal {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "precision",
            "scale",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Precision,
            Scale,
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
                            "precision" => Ok(GeneratedField::Precision),
                            "scale" => Ok(GeneratedField::Scale),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Decimal;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Decimal")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Decimal, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut precision__ = None;
                let mut scale__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Precision => {
                            if precision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("precision"));
                            }
                            precision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Scale => {
                            if scale__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scale"));
                            }
                            scale__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Decimal {
                    precision: precision__.unwrap_or_default(),
                    scale: scale__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Decimal", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DfField {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field.is_some() {
            len += 1;
        }
        if self.qualifier.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.DfField", len)?;
        if let Some(v) = self.field.as_ref() {
            struct_ser.serialize_field("field", v)?;
        }
        if let Some(v) = self.qualifier.as_ref() {
            struct_ser.serialize_field("qualifier", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DfField {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field",
            "qualifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Field,
            Qualifier,
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
                            "field" => Ok(GeneratedField::Field),
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DfField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.DfField")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DfField, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field__ = None;
                let mut qualifier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Field => {
                            if field__.is_some() {
                                return Err(serde::de::Error::duplicate_field("field"));
                            }
                            field__ = map_.next_value()?;
                        }
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = map_.next_value()?;
                        }
                    }
                }
                Ok(DfField {
                    field: field__,
                    qualifier: qualifier__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.DfField", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DfSchema {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        if !self.metadata.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.DfSchema", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        if !self.metadata.is_empty() {
            struct_ser.serialize_field("metadata", &self.metadata)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DfSchema {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
            "metadata",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Columns,
            Metadata,
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
                            "columns" => Ok(GeneratedField::Columns),
                            "metadata" => Ok(GeneratedField::Metadata),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DfSchema;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.DfSchema")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DfSchema, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                let mut metadata__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Metadata => {
                            if metadata__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadata"));
                            }
                            metadata__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(DfSchema {
                    columns: columns__.unwrap_or_default(),
                    metadata: metadata__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.DfSchema", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Dictionary {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.key.is_some() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Dictionary", len)?;
        if let Some(v) = self.key.as_ref() {
            struct_ser.serialize_field("key", v)?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Dictionary {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Key,
            Value,
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
                            "key" => Ok(GeneratedField::Key),
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Dictionary;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Dictionary")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Dictionary, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key__ = None;
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = map_.next_value()?;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Dictionary {
                    key: key__,
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Dictionary", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EmptyMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("datafusion_common.EmptyMessage", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EmptyMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
                            Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EmptyMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.EmptyMessage")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EmptyMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(EmptyMessage {
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.EmptyMessage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Field {
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
        if self.arrow_type.is_some() {
            len += 1;
        }
        if self.nullable {
            len += 1;
        }
        if !self.children.is_empty() {
            len += 1;
        }
        if !self.metadata.is_empty() {
            len += 1;
        }
        if self.dict_id != 0 {
            len += 1;
        }
        if self.dict_ordered {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Field", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        if self.nullable {
            struct_ser.serialize_field("nullable", &self.nullable)?;
        }
        if !self.children.is_empty() {
            struct_ser.serialize_field("children", &self.children)?;
        }
        if !self.metadata.is_empty() {
            struct_ser.serialize_field("metadata", &self.metadata)?;
        }
        if self.dict_id != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("dictId", ToString::to_string(&self.dict_id).as_str())?;
        }
        if self.dict_ordered {
            struct_ser.serialize_field("dictOrdered", &self.dict_ordered)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Field {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "arrow_type",
            "arrowType",
            "nullable",
            "children",
            "metadata",
            "dict_id",
            "dictId",
            "dict_ordered",
            "dictOrdered",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            ArrowType,
            Nullable,
            Children,
            Metadata,
            DictId,
            DictOrdered,
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
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            "nullable" => Ok(GeneratedField::Nullable),
                            "children" => Ok(GeneratedField::Children),
                            "metadata" => Ok(GeneratedField::Metadata),
                            "dictId" | "dict_id" => Ok(GeneratedField::DictId),
                            "dictOrdered" | "dict_ordered" => Ok(GeneratedField::DictOrdered),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Field;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Field")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Field, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut arrow_type__ = None;
                let mut nullable__ = None;
                let mut children__ = None;
                let mut metadata__ = None;
                let mut dict_id__ = None;
                let mut dict_ordered__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map_.next_value()?;
                        }
                        GeneratedField::Nullable => {
                            if nullable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullable"));
                            }
                            nullable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Children => {
                            if children__.is_some() {
                                return Err(serde::de::Error::duplicate_field("children"));
                            }
                            children__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Metadata => {
                            if metadata__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadata"));
                            }
                            metadata__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::DictId => {
                            if dict_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictId"));
                            }
                            dict_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DictOrdered => {
                            if dict_ordered__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictOrdered"));
                            }
                            dict_ordered__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Field {
                    name: name__.unwrap_or_default(),
                    arrow_type: arrow_type__,
                    nullable: nullable__.unwrap_or_default(),
                    children: children__.unwrap_or_default(),
                    metadata: metadata__.unwrap_or_default(),
                    dict_id: dict_id__.unwrap_or_default(),
                    dict_ordered: dict_ordered__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Field", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FixedSizeList {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field_type.is_some() {
            len += 1;
        }
        if self.list_size != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.FixedSizeList", len)?;
        if let Some(v) = self.field_type.as_ref() {
            struct_ser.serialize_field("fieldType", v)?;
        }
        if self.list_size != 0 {
            struct_ser.serialize_field("listSize", &self.list_size)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FixedSizeList {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field_type",
            "fieldType",
            "list_size",
            "listSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FieldType,
            ListSize,
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
                            "fieldType" | "field_type" => Ok(GeneratedField::FieldType),
                            "listSize" | "list_size" => Ok(GeneratedField::ListSize),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FixedSizeList;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.FixedSizeList")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FixedSizeList, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field_type__ = None;
                let mut list_size__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FieldType => {
                            if field_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldType"));
                            }
                            field_type__ = map_.next_value()?;
                        }
                        GeneratedField::ListSize => {
                            if list_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listSize"));
                            }
                            list_size__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FixedSizeList {
                    field_type: field_type__,
                    list_size: list_size__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.FixedSizeList", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IntervalUnit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::YearMonth => "YearMonth",
            Self::DayTime => "DayTime",
            Self::MonthDayNano => "MonthDayNano",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for IntervalUnit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "YearMonth",
            "DayTime",
            "MonthDayNano",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IntervalUnit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "YearMonth" => Ok(IntervalUnit::YearMonth),
                    "DayTime" => Ok(IntervalUnit::DayTime),
                    "MonthDayNano" => Ok(IntervalUnit::MonthDayNano),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for List {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.List", len)?;
        if let Some(v) = self.field_type.as_ref() {
            struct_ser.serialize_field("fieldType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for List {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field_type",
            "fieldType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FieldType,
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
                            "fieldType" | "field_type" => Ok(GeneratedField::FieldType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = List;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.List")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<List, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FieldType => {
                            if field_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldType"));
                            }
                            field_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(List {
                    field_type: field_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.List", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Map {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field_type.is_some() {
            len += 1;
        }
        if self.keys_sorted {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Map", len)?;
        if let Some(v) = self.field_type.as_ref() {
            struct_ser.serialize_field("fieldType", v)?;
        }
        if self.keys_sorted {
            struct_ser.serialize_field("keysSorted", &self.keys_sorted)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Map {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field_type",
            "fieldType",
            "keys_sorted",
            "keysSorted",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FieldType,
            KeysSorted,
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
                            "fieldType" | "field_type" => Ok(GeneratedField::FieldType),
                            "keysSorted" | "keys_sorted" => Ok(GeneratedField::KeysSorted),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Map;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Map")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Map, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field_type__ = None;
                let mut keys_sorted__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FieldType => {
                            if field_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldType"));
                            }
                            field_type__ = map_.next_value()?;
                        }
                        GeneratedField::KeysSorted => {
                            if keys_sorted__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keysSorted"));
                            }
                            keys_sorted__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Map {
                    field_type: field_type__,
                    keys_sorted: keys_sorted__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Map", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Schema {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        if !self.metadata.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Schema", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        if !self.metadata.is_empty() {
            struct_ser.serialize_field("metadata", &self.metadata)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Schema {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
            "metadata",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Columns,
            Metadata,
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
                            "columns" => Ok(GeneratedField::Columns),
                            "metadata" => Ok(GeneratedField::Metadata),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Schema;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Schema")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Schema, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                let mut metadata__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Metadata => {
                            if metadata__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadata"));
                            }
                            metadata__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(Schema {
                    columns: columns__.unwrap_or_default(),
                    metadata: metadata__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Schema", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Struct {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.sub_field_types.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Struct", len)?;
        if !self.sub_field_types.is_empty() {
            struct_ser.serialize_field("subFieldTypes", &self.sub_field_types)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Struct {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sub_field_types",
            "subFieldTypes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SubFieldTypes,
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
                            "subFieldTypes" | "sub_field_types" => Ok(GeneratedField::SubFieldTypes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Struct;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Struct")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Struct, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sub_field_types__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SubFieldTypes => {
                            if sub_field_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subFieldTypes"));
                            }
                            sub_field_types__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Struct {
                    sub_field_types: sub_field_types__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Struct", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TimeUnit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Second => "Second",
            Self::Millisecond => "Millisecond",
            Self::Microsecond => "Microsecond",
            Self::Nanosecond => "Nanosecond",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for TimeUnit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "Second",
            "Millisecond",
            "Microsecond",
            "Nanosecond",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TimeUnit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "Second" => Ok(TimeUnit::Second),
                    "Millisecond" => Ok(TimeUnit::Millisecond),
                    "Microsecond" => Ok(TimeUnit::Microsecond),
                    "Nanosecond" => Ok(TimeUnit::Nanosecond),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Timestamp {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.time_unit != 0 {
            len += 1;
        }
        if !self.timezone.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Timestamp", len)?;
        if self.time_unit != 0 {
            let v = TimeUnit::try_from(self.time_unit)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.time_unit)))?;
            struct_ser.serialize_field("timeUnit", &v)?;
        }
        if !self.timezone.is_empty() {
            struct_ser.serialize_field("timezone", &self.timezone)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Timestamp {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "time_unit",
            "timeUnit",
            "timezone",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TimeUnit,
            Timezone,
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
                            "timeUnit" | "time_unit" => Ok(GeneratedField::TimeUnit),
                            "timezone" => Ok(GeneratedField::Timezone),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Timestamp")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Timestamp, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut time_unit__ = None;
                let mut timezone__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TimeUnit => {
                            if time_unit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeUnit"));
                            }
                            time_unit__ = Some(map_.next_value::<TimeUnit>()? as i32);
                        }
                        GeneratedField::Timezone => {
                            if timezone__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timezone"));
                            }
                            timezone__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Timestamp {
                    time_unit: time_unit__.unwrap_or_default(),
                    timezone: timezone__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Timestamp", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Union {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.union_types.is_empty() {
            len += 1;
        }
        if self.union_mode != 0 {
            len += 1;
        }
        if !self.type_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Union", len)?;
        if !self.union_types.is_empty() {
            struct_ser.serialize_field("unionTypes", &self.union_types)?;
        }
        if self.union_mode != 0 {
            let v = UnionMode::try_from(self.union_mode)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.union_mode)))?;
            struct_ser.serialize_field("unionMode", &v)?;
        }
        if !self.type_ids.is_empty() {
            struct_ser.serialize_field("typeIds", &self.type_ids)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Union {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "union_types",
            "unionTypes",
            "union_mode",
            "unionMode",
            "type_ids",
            "typeIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UnionTypes,
            UnionMode,
            TypeIds,
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
                            "unionTypes" | "union_types" => Ok(GeneratedField::UnionTypes),
                            "unionMode" | "union_mode" => Ok(GeneratedField::UnionMode),
                            "typeIds" | "type_ids" => Ok(GeneratedField::TypeIds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Union;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Union")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Union, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut union_types__ = None;
                let mut union_mode__ = None;
                let mut type_ids__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::UnionTypes => {
                            if union_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unionTypes"));
                            }
                            union_types__ = Some(map_.next_value()?);
                        }
                        GeneratedField::UnionMode => {
                            if union_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unionMode"));
                            }
                            union_mode__ = Some(map_.next_value::<UnionMode>()? as i32);
                        }
                        GeneratedField::TypeIds => {
                            if type_ids__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeIds"));
                            }
                            type_ids__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(Union {
                    union_types: union_types__.unwrap_or_default(),
                    union_mode: union_mode__.unwrap_or_default(),
                    type_ids: type_ids__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Union", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnionMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for UnionMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sparse",
            "dense",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "sparse" => Ok(UnionMode::Sparse),
                    "dense" => Ok(UnionMode::Dense),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
