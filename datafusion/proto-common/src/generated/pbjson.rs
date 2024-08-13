impl serde::Serialize for ArrowOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("datafusion_common.ArrowOptions", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ArrowOptions {
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
            type Value = ArrowOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ArrowOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ArrowOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(ArrowOptions {
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ArrowOptions", FIELDS, GeneratedVisitor)
    }
}
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
                arrow_type::ArrowTypeEnum::Utf8View(v) => {
                    struct_ser.serialize_field("UTF8VIEW", v)?;
                }
                arrow_type::ArrowTypeEnum::LargeUtf8(v) => {
                    struct_ser.serialize_field("LARGEUTF8", v)?;
                }
                arrow_type::ArrowTypeEnum::Binary(v) => {
                    struct_ser.serialize_field("BINARY", v)?;
                }
                arrow_type::ArrowTypeEnum::BinaryView(v) => {
                    struct_ser.serialize_field("BINARYVIEW", v)?;
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
                arrow_type::ArrowTypeEnum::Decimal256(v) => {
                    struct_ser.serialize_field("DECIMAL256", v)?;
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
            "UTF8_VIEW",
            "UTF8VIEW",
            "LARGE_UTF8",
            "LARGEUTF8",
            "BINARY",
            "BINARY_VIEW",
            "BINARYVIEW",
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
            "DECIMAL256",
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
            Utf8View,
            LargeUtf8,
            Binary,
            BinaryView,
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
            Decimal256,
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
                            "UTF8VIEW" | "UTF8_VIEW" => Ok(GeneratedField::Utf8View),
                            "LARGEUTF8" | "LARGE_UTF8" => Ok(GeneratedField::LargeUtf8),
                            "BINARY" => Ok(GeneratedField::Binary),
                            "BINARYVIEW" | "BINARY_VIEW" => Ok(GeneratedField::BinaryView),
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
                            "DECIMAL256" => Ok(GeneratedField::Decimal256),
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
                        GeneratedField::Utf8View => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("UTF8VIEW"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Utf8View)
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
                        GeneratedField::BinaryView => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("BINARYVIEW"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::BinaryView)
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
                        GeneratedField::Decimal256 => {
                            if arrow_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("DECIMAL256"));
                            }
                            arrow_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(arrow_type::ArrowTypeEnum::Decimal256)
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
impl serde::Serialize for AvroFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("datafusion_common.AvroFormat", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AvroFormat {
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
            type Value = AvroFormat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.AvroFormat")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AvroFormat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AvroFormat {
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.AvroFormat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AvroOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("datafusion_common.AvroOptions", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AvroOptions {
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
            type Value = AvroOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.AvroOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AvroOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AvroOptions {
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.AvroOptions", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for ColumnStats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.min_value.is_some() {
            len += 1;
        }
        if self.max_value.is_some() {
            len += 1;
        }
        if self.null_count.is_some() {
            len += 1;
        }
        if self.distinct_count.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ColumnStats", len)?;
        if let Some(v) = self.min_value.as_ref() {
            struct_ser.serialize_field("minValue", v)?;
        }
        if let Some(v) = self.max_value.as_ref() {
            struct_ser.serialize_field("maxValue", v)?;
        }
        if let Some(v) = self.null_count.as_ref() {
            struct_ser.serialize_field("nullCount", v)?;
        }
        if let Some(v) = self.distinct_count.as_ref() {
            struct_ser.serialize_field("distinctCount", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnStats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "min_value",
            "minValue",
            "max_value",
            "maxValue",
            "null_count",
            "nullCount",
            "distinct_count",
            "distinctCount",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            MinValue,
            MaxValue,
            NullCount,
            DistinctCount,
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
                            "minValue" | "min_value" => Ok(GeneratedField::MinValue),
                            "maxValue" | "max_value" => Ok(GeneratedField::MaxValue),
                            "nullCount" | "null_count" => Ok(GeneratedField::NullCount),
                            "distinctCount" | "distinct_count" => Ok(GeneratedField::DistinctCount),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnStats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ColumnStats")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColumnStats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut min_value__ = None;
                let mut max_value__ = None;
                let mut null_count__ = None;
                let mut distinct_count__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::MinValue => {
                            if min_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("minValue"));
                            }
                            min_value__ = map_.next_value()?;
                        }
                        GeneratedField::MaxValue => {
                            if max_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxValue"));
                            }
                            max_value__ = map_.next_value()?;
                        }
                        GeneratedField::NullCount => {
                            if null_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullCount"));
                            }
                            null_count__ = map_.next_value()?;
                        }
                        GeneratedField::DistinctCount => {
                            if distinct_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinctCount"));
                            }
                            distinct_count__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ColumnStats {
                    min_value: min_value__,
                    max_value: max_value__,
                    null_count: null_count__,
                    distinct_count: distinct_count__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ColumnStats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompressionTypeVariant {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Gzip => "GZIP",
            Self::Bzip2 => "BZIP2",
            Self::Xz => "XZ",
            Self::Zstd => "ZSTD",
            Self::Uncompressed => "UNCOMPRESSED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for CompressionTypeVariant {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "GZIP",
            "BZIP2",
            "XZ",
            "ZSTD",
            "UNCOMPRESSED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompressionTypeVariant;

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
                    "GZIP" => Ok(CompressionTypeVariant::Gzip),
                    "BZIP2" => Ok(CompressionTypeVariant::Bzip2),
                    "XZ" => Ok(CompressionTypeVariant::Xz),
                    "ZSTD" => Ok(CompressionTypeVariant::Zstd),
                    "UNCOMPRESSED" => Ok(CompressionTypeVariant::Uncompressed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Constraint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.constraint_mode.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Constraint", len)?;
        if let Some(v) = self.constraint_mode.as_ref() {
            match v {
                constraint::ConstraintMode::PrimaryKey(v) => {
                    struct_ser.serialize_field("primaryKey", v)?;
                }
                constraint::ConstraintMode::Unique(v) => {
                    struct_ser.serialize_field("unique", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Constraint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "primary_key",
            "primaryKey",
            "unique",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PrimaryKey,
            Unique,
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
                            "primaryKey" | "primary_key" => Ok(GeneratedField::PrimaryKey),
                            "unique" => Ok(GeneratedField::Unique),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Constraint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Constraint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Constraint, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut constraint_mode__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::PrimaryKey => {
                            if constraint_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("primaryKey"));
                            }
                            constraint_mode__ = map_.next_value::<::std::option::Option<_>>()?.map(constraint::ConstraintMode::PrimaryKey)
;
                        }
                        GeneratedField::Unique => {
                            if constraint_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unique"));
                            }
                            constraint_mode__ = map_.next_value::<::std::option::Option<_>>()?.map(constraint::ConstraintMode::Unique)
;
                        }
                    }
                }
                Ok(Constraint {
                    constraint_mode: constraint_mode__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Constraint", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Constraints {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.constraints.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Constraints", len)?;
        if !self.constraints.is_empty() {
            struct_ser.serialize_field("constraints", &self.constraints)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Constraints {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "constraints",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Constraints,
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
                            "constraints" => Ok(GeneratedField::Constraints),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Constraints;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Constraints")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Constraints, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut constraints__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Constraints => {
                            if constraints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("constraints"));
                            }
                            constraints__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Constraints {
                    constraints: constraints__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Constraints", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.CsvFormat", len)?;
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvFormat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Options,
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
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvFormat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.CsvFormat")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CsvFormat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CsvFormat {
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.CsvFormat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.has_header.is_empty() {
            len += 1;
        }
        if !self.delimiter.is_empty() {
            len += 1;
        }
        if !self.quote.is_empty() {
            len += 1;
        }
        if !self.escape.is_empty() {
            len += 1;
        }
        if self.compression != 0 {
            len += 1;
        }
        if self.schema_infer_max_rec != 0 {
            len += 1;
        }
        if !self.date_format.is_empty() {
            len += 1;
        }
        if !self.datetime_format.is_empty() {
            len += 1;
        }
        if !self.timestamp_format.is_empty() {
            len += 1;
        }
        if !self.timestamp_tz_format.is_empty() {
            len += 1;
        }
        if !self.time_format.is_empty() {
            len += 1;
        }
        if !self.null_value.is_empty() {
            len += 1;
        }
        if !self.comment.is_empty() {
            len += 1;
        }
        if !self.double_quote.is_empty() {
            len += 1;
        }
        if !self.newlines_in_values.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.CsvOptions", len)?;
        if !self.has_header.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("hasHeader", pbjson::private::base64::encode(&self.has_header).as_str())?;
        }
        if !self.delimiter.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("delimiter", pbjson::private::base64::encode(&self.delimiter).as_str())?;
        }
        if !self.quote.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("quote", pbjson::private::base64::encode(&self.quote).as_str())?;
        }
        if !self.escape.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("escape", pbjson::private::base64::encode(&self.escape).as_str())?;
        }
        if self.compression != 0 {
            let v = CompressionTypeVariant::try_from(self.compression)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compression)))?;
            struct_ser.serialize_field("compression", &v)?;
        }
        if self.schema_infer_max_rec != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("schemaInferMaxRec", ToString::to_string(&self.schema_infer_max_rec).as_str())?;
        }
        if !self.date_format.is_empty() {
            struct_ser.serialize_field("dateFormat", &self.date_format)?;
        }
        if !self.datetime_format.is_empty() {
            struct_ser.serialize_field("datetimeFormat", &self.datetime_format)?;
        }
        if !self.timestamp_format.is_empty() {
            struct_ser.serialize_field("timestampFormat", &self.timestamp_format)?;
        }
        if !self.timestamp_tz_format.is_empty() {
            struct_ser.serialize_field("timestampTzFormat", &self.timestamp_tz_format)?;
        }
        if !self.time_format.is_empty() {
            struct_ser.serialize_field("timeFormat", &self.time_format)?;
        }
        if !self.null_value.is_empty() {
            struct_ser.serialize_field("nullValue", &self.null_value)?;
        }
        if !self.comment.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("comment", pbjson::private::base64::encode(&self.comment).as_str())?;
        }
        if !self.double_quote.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("doubleQuote", pbjson::private::base64::encode(&self.double_quote).as_str())?;
        }
        if !self.newlines_in_values.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("newlinesInValues", pbjson::private::base64::encode(&self.newlines_in_values).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "has_header",
            "hasHeader",
            "delimiter",
            "quote",
            "escape",
            "compression",
            "schema_infer_max_rec",
            "schemaInferMaxRec",
            "date_format",
            "dateFormat",
            "datetime_format",
            "datetimeFormat",
            "timestamp_format",
            "timestampFormat",
            "timestamp_tz_format",
            "timestampTzFormat",
            "time_format",
            "timeFormat",
            "null_value",
            "nullValue",
            "comment",
            "double_quote",
            "doubleQuote",
            "newlines_in_values",
            "newlinesInValues",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HasHeader,
            Delimiter,
            Quote,
            Escape,
            Compression,
            SchemaInferMaxRec,
            DateFormat,
            DatetimeFormat,
            TimestampFormat,
            TimestampTzFormat,
            TimeFormat,
            NullValue,
            Comment,
            DoubleQuote,
            NewlinesInValues,
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
                            "hasHeader" | "has_header" => Ok(GeneratedField::HasHeader),
                            "delimiter" => Ok(GeneratedField::Delimiter),
                            "quote" => Ok(GeneratedField::Quote),
                            "escape" => Ok(GeneratedField::Escape),
                            "compression" => Ok(GeneratedField::Compression),
                            "schemaInferMaxRec" | "schema_infer_max_rec" => Ok(GeneratedField::SchemaInferMaxRec),
                            "dateFormat" | "date_format" => Ok(GeneratedField::DateFormat),
                            "datetimeFormat" | "datetime_format" => Ok(GeneratedField::DatetimeFormat),
                            "timestampFormat" | "timestamp_format" => Ok(GeneratedField::TimestampFormat),
                            "timestampTzFormat" | "timestamp_tz_format" => Ok(GeneratedField::TimestampTzFormat),
                            "timeFormat" | "time_format" => Ok(GeneratedField::TimeFormat),
                            "nullValue" | "null_value" => Ok(GeneratedField::NullValue),
                            "comment" => Ok(GeneratedField::Comment),
                            "doubleQuote" | "double_quote" => Ok(GeneratedField::DoubleQuote),
                            "newlinesInValues" | "newlines_in_values" => Ok(GeneratedField::NewlinesInValues),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.CsvOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CsvOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut has_header__ = None;
                let mut delimiter__ = None;
                let mut quote__ = None;
                let mut escape__ = None;
                let mut compression__ = None;
                let mut schema_infer_max_rec__ = None;
                let mut date_format__ = None;
                let mut datetime_format__ = None;
                let mut timestamp_format__ = None;
                let mut timestamp_tz_format__ = None;
                let mut time_format__ = None;
                let mut null_value__ = None;
                let mut comment__ = None;
                let mut double_quote__ = None;
                let mut newlines_in_values__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::HasHeader => {
                            if has_header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hasHeader"));
                            }
                            has_header__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Delimiter => {
                            if delimiter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delimiter"));
                            }
                            delimiter__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Quote => {
                            if quote__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quote"));
                            }
                            quote__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Escape => {
                            if escape__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escape"));
                            }
                            escape__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Compression => {
                            if compression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression__ = Some(map_.next_value::<CompressionTypeVariant>()? as i32);
                        }
                        GeneratedField::SchemaInferMaxRec => {
                            if schema_infer_max_rec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaInferMaxRec"));
                            }
                            schema_infer_max_rec__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DateFormat => {
                            if date_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dateFormat"));
                            }
                            date_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DatetimeFormat => {
                            if datetime_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("datetimeFormat"));
                            }
                            datetime_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TimestampFormat => {
                            if timestamp_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestampFormat"));
                            }
                            timestamp_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TimestampTzFormat => {
                            if timestamp_tz_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestampTzFormat"));
                            }
                            timestamp_tz_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TimeFormat => {
                            if time_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeFormat"));
                            }
                            time_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NullValue => {
                            if null_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullValue"));
                            }
                            null_value__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Comment => {
                            if comment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("comment"));
                            }
                            comment__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DoubleQuote => {
                            if double_quote__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doubleQuote"));
                            }
                            double_quote__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NewlinesInValues => {
                            if newlines_in_values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("newlinesInValues"));
                            }
                            newlines_in_values__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(CsvOptions {
                    has_header: has_header__.unwrap_or_default(),
                    delimiter: delimiter__.unwrap_or_default(),
                    quote: quote__.unwrap_or_default(),
                    escape: escape__.unwrap_or_default(),
                    compression: compression__.unwrap_or_default(),
                    schema_infer_max_rec: schema_infer_max_rec__.unwrap_or_default(),
                    date_format: date_format__.unwrap_or_default(),
                    datetime_format: datetime_format__.unwrap_or_default(),
                    timestamp_format: timestamp_format__.unwrap_or_default(),
                    timestamp_tz_format: timestamp_tz_format__.unwrap_or_default(),
                    time_format: time_format__.unwrap_or_default(),
                    null_value: null_value__.unwrap_or_default(),
                    comment: comment__.unwrap_or_default(),
                    double_quote: double_quote__.unwrap_or_default(),
                    newlines_in_values: newlines_in_values__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.CsvOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvWriterOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.compression != 0 {
            len += 1;
        }
        if !self.delimiter.is_empty() {
            len += 1;
        }
        if self.has_header {
            len += 1;
        }
        if !self.date_format.is_empty() {
            len += 1;
        }
        if !self.datetime_format.is_empty() {
            len += 1;
        }
        if !self.timestamp_format.is_empty() {
            len += 1;
        }
        if !self.time_format.is_empty() {
            len += 1;
        }
        if !self.null_value.is_empty() {
            len += 1;
        }
        if !self.quote.is_empty() {
            len += 1;
        }
        if !self.escape.is_empty() {
            len += 1;
        }
        if self.double_quote {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.CsvWriterOptions", len)?;
        if self.compression != 0 {
            let v = CompressionTypeVariant::try_from(self.compression)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compression)))?;
            struct_ser.serialize_field("compression", &v)?;
        }
        if !self.delimiter.is_empty() {
            struct_ser.serialize_field("delimiter", &self.delimiter)?;
        }
        if self.has_header {
            struct_ser.serialize_field("hasHeader", &self.has_header)?;
        }
        if !self.date_format.is_empty() {
            struct_ser.serialize_field("dateFormat", &self.date_format)?;
        }
        if !self.datetime_format.is_empty() {
            struct_ser.serialize_field("datetimeFormat", &self.datetime_format)?;
        }
        if !self.timestamp_format.is_empty() {
            struct_ser.serialize_field("timestampFormat", &self.timestamp_format)?;
        }
        if !self.time_format.is_empty() {
            struct_ser.serialize_field("timeFormat", &self.time_format)?;
        }
        if !self.null_value.is_empty() {
            struct_ser.serialize_field("nullValue", &self.null_value)?;
        }
        if !self.quote.is_empty() {
            struct_ser.serialize_field("quote", &self.quote)?;
        }
        if !self.escape.is_empty() {
            struct_ser.serialize_field("escape", &self.escape)?;
        }
        if self.double_quote {
            struct_ser.serialize_field("doubleQuote", &self.double_quote)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvWriterOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "compression",
            "delimiter",
            "has_header",
            "hasHeader",
            "date_format",
            "dateFormat",
            "datetime_format",
            "datetimeFormat",
            "timestamp_format",
            "timestampFormat",
            "time_format",
            "timeFormat",
            "null_value",
            "nullValue",
            "quote",
            "escape",
            "double_quote",
            "doubleQuote",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Compression,
            Delimiter,
            HasHeader,
            DateFormat,
            DatetimeFormat,
            TimestampFormat,
            TimeFormat,
            NullValue,
            Quote,
            Escape,
            DoubleQuote,
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
                            "compression" => Ok(GeneratedField::Compression),
                            "delimiter" => Ok(GeneratedField::Delimiter),
                            "hasHeader" | "has_header" => Ok(GeneratedField::HasHeader),
                            "dateFormat" | "date_format" => Ok(GeneratedField::DateFormat),
                            "datetimeFormat" | "datetime_format" => Ok(GeneratedField::DatetimeFormat),
                            "timestampFormat" | "timestamp_format" => Ok(GeneratedField::TimestampFormat),
                            "timeFormat" | "time_format" => Ok(GeneratedField::TimeFormat),
                            "nullValue" | "null_value" => Ok(GeneratedField::NullValue),
                            "quote" => Ok(GeneratedField::Quote),
                            "escape" => Ok(GeneratedField::Escape),
                            "doubleQuote" | "double_quote" => Ok(GeneratedField::DoubleQuote),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvWriterOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.CsvWriterOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CsvWriterOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compression__ = None;
                let mut delimiter__ = None;
                let mut has_header__ = None;
                let mut date_format__ = None;
                let mut datetime_format__ = None;
                let mut timestamp_format__ = None;
                let mut time_format__ = None;
                let mut null_value__ = None;
                let mut quote__ = None;
                let mut escape__ = None;
                let mut double_quote__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Compression => {
                            if compression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression__ = Some(map_.next_value::<CompressionTypeVariant>()? as i32);
                        }
                        GeneratedField::Delimiter => {
                            if delimiter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delimiter"));
                            }
                            delimiter__ = Some(map_.next_value()?);
                        }
                        GeneratedField::HasHeader => {
                            if has_header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hasHeader"));
                            }
                            has_header__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DateFormat => {
                            if date_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dateFormat"));
                            }
                            date_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DatetimeFormat => {
                            if datetime_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("datetimeFormat"));
                            }
                            datetime_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TimestampFormat => {
                            if timestamp_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestampFormat"));
                            }
                            timestamp_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TimeFormat => {
                            if time_format__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeFormat"));
                            }
                            time_format__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NullValue => {
                            if null_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullValue"));
                            }
                            null_value__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Quote => {
                            if quote__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quote"));
                            }
                            quote__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Escape => {
                            if escape__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escape"));
                            }
                            escape__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DoubleQuote => {
                            if double_quote__.is_some() {
                                return Err(serde::de::Error::duplicate_field("doubleQuote"));
                            }
                            double_quote__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CsvWriterOptions {
                    compression: compression__.unwrap_or_default(),
                    delimiter: delimiter__.unwrap_or_default(),
                    has_header: has_header__.unwrap_or_default(),
                    date_format: date_format__.unwrap_or_default(),
                    datetime_format: datetime_format__.unwrap_or_default(),
                    timestamp_format: timestamp_format__.unwrap_or_default(),
                    time_format: time_format__.unwrap_or_default(),
                    null_value: null_value__.unwrap_or_default(),
                    quote: quote__.unwrap_or_default(),
                    escape: escape__.unwrap_or_default(),
                    double_quote: double_quote__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.CsvWriterOptions", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for Decimal128 {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.value.is_empty() {
            len += 1;
        }
        if self.p != 0 {
            len += 1;
        }
        if self.s != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Decimal128", len)?;
        if !self.value.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("value", pbjson::private::base64::encode(&self.value).as_str())?;
        }
        if self.p != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("p", ToString::to_string(&self.p).as_str())?;
        }
        if self.s != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("s", ToString::to_string(&self.s).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Decimal128 {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "value",
            "p",
            "s",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Value,
            P,
            S,
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
                            "value" => Ok(GeneratedField::Value),
                            "p" => Ok(GeneratedField::P),
                            "s" => Ok(GeneratedField::S),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Decimal128;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Decimal128")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Decimal128, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                let mut p__ = None;
                let mut s__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::P => {
                            if p__.is_some() {
                                return Err(serde::de::Error::duplicate_field("p"));
                            }
                            p__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::S => {
                            if s__.is_some() {
                                return Err(serde::de::Error::duplicate_field("s"));
                            }
                            s__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Decimal128 {
                    value: value__.unwrap_or_default(),
                    p: p__.unwrap_or_default(),
                    s: s__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Decimal128", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Decimal256 {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.value.is_empty() {
            len += 1;
        }
        if self.p != 0 {
            len += 1;
        }
        if self.s != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Decimal256", len)?;
        if !self.value.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("value", pbjson::private::base64::encode(&self.value).as_str())?;
        }
        if self.p != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("p", ToString::to_string(&self.p).as_str())?;
        }
        if self.s != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("s", ToString::to_string(&self.s).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Decimal256 {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "value",
            "p",
            "s",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Value,
            P,
            S,
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
                            "value" => Ok(GeneratedField::Value),
                            "p" => Ok(GeneratedField::P),
                            "s" => Ok(GeneratedField::S),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Decimal256;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Decimal256")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Decimal256, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                let mut p__ = None;
                let mut s__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::P => {
                            if p__.is_some() {
                                return Err(serde::de::Error::duplicate_field("p"));
                            }
                            p__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::S => {
                            if s__.is_some() {
                                return Err(serde::de::Error::duplicate_field("s"));
                            }
                            s__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Decimal256 {
                    value: value__.unwrap_or_default(),
                    p: p__.unwrap_or_default(),
                    s: s__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Decimal256", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Decimal256Type {
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
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Decimal256Type", len)?;
        if self.precision != 0 {
            struct_ser.serialize_field("precision", &self.precision)?;
        }
        if self.scale != 0 {
            struct_ser.serialize_field("scale", &self.scale)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Decimal256Type {
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
            type Value = Decimal256Type;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Decimal256Type")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Decimal256Type, V::Error>
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
                Ok(Decimal256Type {
                    precision: precision__.unwrap_or_default(),
                    scale: scale__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Decimal256Type", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for IntervalDayTimeValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.days != 0 {
            len += 1;
        }
        if self.milliseconds != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.IntervalDayTimeValue", len)?;
        if self.days != 0 {
            struct_ser.serialize_field("days", &self.days)?;
        }
        if self.milliseconds != 0 {
            struct_ser.serialize_field("milliseconds", &self.milliseconds)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IntervalDayTimeValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "days",
            "milliseconds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Days,
            Milliseconds,
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
                            "days" => Ok(GeneratedField::Days),
                            "milliseconds" => Ok(GeneratedField::Milliseconds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IntervalDayTimeValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.IntervalDayTimeValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IntervalDayTimeValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut days__ = None;
                let mut milliseconds__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Days => {
                            if days__.is_some() {
                                return Err(serde::de::Error::duplicate_field("days"));
                            }
                            days__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Milliseconds => {
                            if milliseconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("milliseconds"));
                            }
                            milliseconds__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(IntervalDayTimeValue {
                    days: days__.unwrap_or_default(),
                    milliseconds: milliseconds__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.IntervalDayTimeValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IntervalMonthDayNanoValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.months != 0 {
            len += 1;
        }
        if self.days != 0 {
            len += 1;
        }
        if self.nanos != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.IntervalMonthDayNanoValue", len)?;
        if self.months != 0 {
            struct_ser.serialize_field("months", &self.months)?;
        }
        if self.days != 0 {
            struct_ser.serialize_field("days", &self.days)?;
        }
        if self.nanos != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("nanos", ToString::to_string(&self.nanos).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IntervalMonthDayNanoValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "months",
            "days",
            "nanos",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Months,
            Days,
            Nanos,
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
                            "months" => Ok(GeneratedField::Months),
                            "days" => Ok(GeneratedField::Days),
                            "nanos" => Ok(GeneratedField::Nanos),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IntervalMonthDayNanoValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.IntervalMonthDayNanoValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IntervalMonthDayNanoValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut months__ = None;
                let mut days__ = None;
                let mut nanos__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Months => {
                            if months__.is_some() {
                                return Err(serde::de::Error::duplicate_field("months"));
                            }
                            months__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Days => {
                            if days__.is_some() {
                                return Err(serde::de::Error::duplicate_field("days"));
                            }
                            days__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Nanos => {
                            if nanos__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nanos"));
                            }
                            nanos__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(IntervalMonthDayNanoValue {
                    months: months__.unwrap_or_default(),
                    days: days__.unwrap_or_default(),
                    nanos: nanos__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.IntervalMonthDayNanoValue", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for JoinConstraint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::On => "ON",
            Self::Using => "USING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for JoinConstraint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ON",
            "USING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinConstraint;

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
                    "ON" => Ok(JoinConstraint::On),
                    "USING" => Ok(JoinConstraint::Using),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for JoinSide {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::LeftSide => "LEFT_SIDE",
            Self::RightSide => "RIGHT_SIDE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for JoinSide {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "LEFT_SIDE",
            "RIGHT_SIDE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinSide;

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
                    "LEFT_SIDE" => Ok(JoinSide::LeftSide),
                    "RIGHT_SIDE" => Ok(JoinSide::RightSide),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for JoinType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Inner => "INNER",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::Full => "FULL",
            Self::Leftsemi => "LEFTSEMI",
            Self::Leftanti => "LEFTANTI",
            Self::Rightsemi => "RIGHTSEMI",
            Self::Rightanti => "RIGHTANTI",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for JoinType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "LEFTSEMI",
            "LEFTANTI",
            "RIGHTSEMI",
            "RIGHTANTI",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinType;

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
                    "INNER" => Ok(JoinType::Inner),
                    "LEFT" => Ok(JoinType::Left),
                    "RIGHT" => Ok(JoinType::Right),
                    "FULL" => Ok(JoinType::Full),
                    "LEFTSEMI" => Ok(JoinType::Leftsemi),
                    "LEFTANTI" => Ok(JoinType::Leftanti),
                    "RIGHTSEMI" => Ok(JoinType::Rightsemi),
                    "RIGHTANTI" => Ok(JoinType::Rightanti),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for JsonOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.compression != 0 {
            len += 1;
        }
        if self.schema_infer_max_rec != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.JsonOptions", len)?;
        if self.compression != 0 {
            let v = CompressionTypeVariant::try_from(self.compression)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compression)))?;
            struct_ser.serialize_field("compression", &v)?;
        }
        if self.schema_infer_max_rec != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("schemaInferMaxRec", ToString::to_string(&self.schema_infer_max_rec).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JsonOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "compression",
            "schema_infer_max_rec",
            "schemaInferMaxRec",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Compression,
            SchemaInferMaxRec,
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
                            "compression" => Ok(GeneratedField::Compression),
                            "schemaInferMaxRec" | "schema_infer_max_rec" => Ok(GeneratedField::SchemaInferMaxRec),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JsonOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.JsonOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JsonOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compression__ = None;
                let mut schema_infer_max_rec__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Compression => {
                            if compression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression__ = Some(map_.next_value::<CompressionTypeVariant>()? as i32);
                        }
                        GeneratedField::SchemaInferMaxRec => {
                            if schema_infer_max_rec__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaInferMaxRec"));
                            }
                            schema_infer_max_rec__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(JsonOptions {
                    compression: compression__.unwrap_or_default(),
                    schema_infer_max_rec: schema_infer_max_rec__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.JsonOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JsonWriterOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.compression != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.JsonWriterOptions", len)?;
        if self.compression != 0 {
            let v = CompressionTypeVariant::try_from(self.compression)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compression)))?;
            struct_ser.serialize_field("compression", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JsonWriterOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "compression",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Compression,
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
                            "compression" => Ok(GeneratedField::Compression),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JsonWriterOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.JsonWriterOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JsonWriterOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compression__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Compression => {
                            if compression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression__ = Some(map_.next_value::<CompressionTypeVariant>()? as i32);
                        }
                    }
                }
                Ok(JsonWriterOptions {
                    compression: compression__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.JsonWriterOptions", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for NdJsonFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.NdJsonFormat", len)?;
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NdJsonFormat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Options,
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
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NdJsonFormat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.NdJsonFormat")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NdJsonFormat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(NdJsonFormat {
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.NdJsonFormat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetColumnOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.bloom_filter_enabled_opt.is_some() {
            len += 1;
        }
        if self.encoding_opt.is_some() {
            len += 1;
        }
        if self.dictionary_enabled_opt.is_some() {
            len += 1;
        }
        if self.compression_opt.is_some() {
            len += 1;
        }
        if self.statistics_enabled_opt.is_some() {
            len += 1;
        }
        if self.bloom_filter_fpp_opt.is_some() {
            len += 1;
        }
        if self.bloom_filter_ndv_opt.is_some() {
            len += 1;
        }
        if self.max_statistics_size_opt.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ParquetColumnOptions", len)?;
        if let Some(v) = self.bloom_filter_enabled_opt.as_ref() {
            match v {
                parquet_column_options::BloomFilterEnabledOpt::BloomFilterEnabled(v) => {
                    struct_ser.serialize_field("bloomFilterEnabled", v)?;
                }
            }
        }
        if let Some(v) = self.encoding_opt.as_ref() {
            match v {
                parquet_column_options::EncodingOpt::Encoding(v) => {
                    struct_ser.serialize_field("encoding", v)?;
                }
            }
        }
        if let Some(v) = self.dictionary_enabled_opt.as_ref() {
            match v {
                parquet_column_options::DictionaryEnabledOpt::DictionaryEnabled(v) => {
                    struct_ser.serialize_field("dictionaryEnabled", v)?;
                }
            }
        }
        if let Some(v) = self.compression_opt.as_ref() {
            match v {
                parquet_column_options::CompressionOpt::Compression(v) => {
                    struct_ser.serialize_field("compression", v)?;
                }
            }
        }
        if let Some(v) = self.statistics_enabled_opt.as_ref() {
            match v {
                parquet_column_options::StatisticsEnabledOpt::StatisticsEnabled(v) => {
                    struct_ser.serialize_field("statisticsEnabled", v)?;
                }
            }
        }
        if let Some(v) = self.bloom_filter_fpp_opt.as_ref() {
            match v {
                parquet_column_options::BloomFilterFppOpt::BloomFilterFpp(v) => {
                    struct_ser.serialize_field("bloomFilterFpp", v)?;
                }
            }
        }
        if let Some(v) = self.bloom_filter_ndv_opt.as_ref() {
            match v {
                parquet_column_options::BloomFilterNdvOpt::BloomFilterNdv(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("bloomFilterNdv", ToString::to_string(&v).as_str())?;
                }
            }
        }
        if let Some(v) = self.max_statistics_size_opt.as_ref() {
            match v {
                parquet_column_options::MaxStatisticsSizeOpt::MaxStatisticsSize(v) => {
                    struct_ser.serialize_field("maxStatisticsSize", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetColumnOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bloom_filter_enabled",
            "bloomFilterEnabled",
            "encoding",
            "dictionary_enabled",
            "dictionaryEnabled",
            "compression",
            "statistics_enabled",
            "statisticsEnabled",
            "bloom_filter_fpp",
            "bloomFilterFpp",
            "bloom_filter_ndv",
            "bloomFilterNdv",
            "max_statistics_size",
            "maxStatisticsSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BloomFilterEnabled,
            Encoding,
            DictionaryEnabled,
            Compression,
            StatisticsEnabled,
            BloomFilterFpp,
            BloomFilterNdv,
            MaxStatisticsSize,
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
                            "bloomFilterEnabled" | "bloom_filter_enabled" => Ok(GeneratedField::BloomFilterEnabled),
                            "encoding" => Ok(GeneratedField::Encoding),
                            "dictionaryEnabled" | "dictionary_enabled" => Ok(GeneratedField::DictionaryEnabled),
                            "compression" => Ok(GeneratedField::Compression),
                            "statisticsEnabled" | "statistics_enabled" => Ok(GeneratedField::StatisticsEnabled),
                            "bloomFilterFpp" | "bloom_filter_fpp" => Ok(GeneratedField::BloomFilterFpp),
                            "bloomFilterNdv" | "bloom_filter_ndv" => Ok(GeneratedField::BloomFilterNdv),
                            "maxStatisticsSize" | "max_statistics_size" => Ok(GeneratedField::MaxStatisticsSize),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetColumnOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ParquetColumnOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ParquetColumnOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut bloom_filter_enabled_opt__ = None;
                let mut encoding_opt__ = None;
                let mut dictionary_enabled_opt__ = None;
                let mut compression_opt__ = None;
                let mut statistics_enabled_opt__ = None;
                let mut bloom_filter_fpp_opt__ = None;
                let mut bloom_filter_ndv_opt__ = None;
                let mut max_statistics_size_opt__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::BloomFilterEnabled => {
                            if bloom_filter_enabled_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomFilterEnabled"));
                            }
                            bloom_filter_enabled_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_column_options::BloomFilterEnabledOpt::BloomFilterEnabled);
                        }
                        GeneratedField::Encoding => {
                            if encoding_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("encoding"));
                            }
                            encoding_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_column_options::EncodingOpt::Encoding);
                        }
                        GeneratedField::DictionaryEnabled => {
                            if dictionary_enabled_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictionaryEnabled"));
                            }
                            dictionary_enabled_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_column_options::DictionaryEnabledOpt::DictionaryEnabled);
                        }
                        GeneratedField::Compression => {
                            if compression_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_column_options::CompressionOpt::Compression);
                        }
                        GeneratedField::StatisticsEnabled => {
                            if statistics_enabled_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("statisticsEnabled"));
                            }
                            statistics_enabled_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_column_options::StatisticsEnabledOpt::StatisticsEnabled);
                        }
                        GeneratedField::BloomFilterFpp => {
                            if bloom_filter_fpp_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomFilterFpp"));
                            }
                            bloom_filter_fpp_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_column_options::BloomFilterFppOpt::BloomFilterFpp(x.0));
                        }
                        GeneratedField::BloomFilterNdv => {
                            if bloom_filter_ndv_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomFilterNdv"));
                            }
                            bloom_filter_ndv_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_column_options::BloomFilterNdvOpt::BloomFilterNdv(x.0));
                        }
                        GeneratedField::MaxStatisticsSize => {
                            if max_statistics_size_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxStatisticsSize"));
                            }
                            max_statistics_size_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_column_options::MaxStatisticsSizeOpt::MaxStatisticsSize(x.0));
                        }
                    }
                }
                Ok(ParquetColumnOptions {
                    bloom_filter_enabled_opt: bloom_filter_enabled_opt__,
                    encoding_opt: encoding_opt__,
                    dictionary_enabled_opt: dictionary_enabled_opt__,
                    compression_opt: compression_opt__,
                    statistics_enabled_opt: statistics_enabled_opt__,
                    bloom_filter_fpp_opt: bloom_filter_fpp_opt__,
                    bloom_filter_ndv_opt: bloom_filter_ndv_opt__,
                    max_statistics_size_opt: max_statistics_size_opt__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ParquetColumnOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetColumnSpecificOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.column_name.is_empty() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ParquetColumnSpecificOptions", len)?;
        if !self.column_name.is_empty() {
            struct_ser.serialize_field("columnName", &self.column_name)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetColumnSpecificOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column_name",
            "columnName",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnName,
            Options,
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
                            "columnName" | "column_name" => Ok(GeneratedField::ColumnName),
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetColumnSpecificOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ParquetColumnSpecificOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ParquetColumnSpecificOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_name__ = None;
                let mut options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ColumnName => {
                            if column_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnName"));
                            }
                            column_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ParquetColumnSpecificOptions {
                    column_name: column_name__.unwrap_or_default(),
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ParquetColumnSpecificOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetFormat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ParquetFormat", len)?;
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetFormat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Options,
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
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetFormat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ParquetFormat")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ParquetFormat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ParquetFormat {
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ParquetFormat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.enable_page_index {
            len += 1;
        }
        if self.pruning {
            len += 1;
        }
        if self.skip_metadata {
            len += 1;
        }
        if self.pushdown_filters {
            len += 1;
        }
        if self.reorder_filters {
            len += 1;
        }
        if self.data_pagesize_limit != 0 {
            len += 1;
        }
        if self.write_batch_size != 0 {
            len += 1;
        }
        if !self.writer_version.is_empty() {
            len += 1;
        }
        if self.allow_single_file_parallelism {
            len += 1;
        }
        if self.maximum_parallel_row_group_writers != 0 {
            len += 1;
        }
        if self.maximum_buffered_record_batches_per_stream != 0 {
            len += 1;
        }
        if self.bloom_filter_on_read {
            len += 1;
        }
        if self.bloom_filter_on_write {
            len += 1;
        }
        if self.schema_force_string_view {
            len += 1;
        }
        if self.dictionary_page_size_limit != 0 {
            len += 1;
        }
        if self.data_page_row_count_limit != 0 {
            len += 1;
        }
        if self.max_row_group_size != 0 {
            len += 1;
        }
        if !self.created_by.is_empty() {
            len += 1;
        }
        if self.metadata_size_hint_opt.is_some() {
            len += 1;
        }
        if self.compression_opt.is_some() {
            len += 1;
        }
        if self.dictionary_enabled_opt.is_some() {
            len += 1;
        }
        if self.statistics_enabled_opt.is_some() {
            len += 1;
        }
        if self.max_statistics_size_opt.is_some() {
            len += 1;
        }
        if self.column_index_truncate_length_opt.is_some() {
            len += 1;
        }
        if self.encoding_opt.is_some() {
            len += 1;
        }
        if self.bloom_filter_fpp_opt.is_some() {
            len += 1;
        }
        if self.bloom_filter_ndv_opt.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ParquetOptions", len)?;
        if self.enable_page_index {
            struct_ser.serialize_field("enablePageIndex", &self.enable_page_index)?;
        }
        if self.pruning {
            struct_ser.serialize_field("pruning", &self.pruning)?;
        }
        if self.skip_metadata {
            struct_ser.serialize_field("skipMetadata", &self.skip_metadata)?;
        }
        if self.pushdown_filters {
            struct_ser.serialize_field("pushdownFilters", &self.pushdown_filters)?;
        }
        if self.reorder_filters {
            struct_ser.serialize_field("reorderFilters", &self.reorder_filters)?;
        }
        if self.data_pagesize_limit != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("dataPagesizeLimit", ToString::to_string(&self.data_pagesize_limit).as_str())?;
        }
        if self.write_batch_size != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("writeBatchSize", ToString::to_string(&self.write_batch_size).as_str())?;
        }
        if !self.writer_version.is_empty() {
            struct_ser.serialize_field("writerVersion", &self.writer_version)?;
        }
        if self.allow_single_file_parallelism {
            struct_ser.serialize_field("allowSingleFileParallelism", &self.allow_single_file_parallelism)?;
        }
        if self.maximum_parallel_row_group_writers != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("maximumParallelRowGroupWriters", ToString::to_string(&self.maximum_parallel_row_group_writers).as_str())?;
        }
        if self.maximum_buffered_record_batches_per_stream != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("maximumBufferedRecordBatchesPerStream", ToString::to_string(&self.maximum_buffered_record_batches_per_stream).as_str())?;
        }
        if self.bloom_filter_on_read {
            struct_ser.serialize_field("bloomFilterOnRead", &self.bloom_filter_on_read)?;
        }
        if self.bloom_filter_on_write {
            struct_ser.serialize_field("bloomFilterOnWrite", &self.bloom_filter_on_write)?;
        }
        if self.schema_force_string_view {
            struct_ser.serialize_field("schemaForceStringView", &self.schema_force_string_view)?;
        }
        if self.dictionary_page_size_limit != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("dictionaryPageSizeLimit", ToString::to_string(&self.dictionary_page_size_limit).as_str())?;
        }
        if self.data_page_row_count_limit != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("dataPageRowCountLimit", ToString::to_string(&self.data_page_row_count_limit).as_str())?;
        }
        if self.max_row_group_size != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("maxRowGroupSize", ToString::to_string(&self.max_row_group_size).as_str())?;
        }
        if !self.created_by.is_empty() {
            struct_ser.serialize_field("createdBy", &self.created_by)?;
        }
        if let Some(v) = self.metadata_size_hint_opt.as_ref() {
            match v {
                parquet_options::MetadataSizeHintOpt::MetadataSizeHint(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("metadataSizeHint", ToString::to_string(&v).as_str())?;
                }
            }
        }
        if let Some(v) = self.compression_opt.as_ref() {
            match v {
                parquet_options::CompressionOpt::Compression(v) => {
                    struct_ser.serialize_field("compression", v)?;
                }
            }
        }
        if let Some(v) = self.dictionary_enabled_opt.as_ref() {
            match v {
                parquet_options::DictionaryEnabledOpt::DictionaryEnabled(v) => {
                    struct_ser.serialize_field("dictionaryEnabled", v)?;
                }
            }
        }
        if let Some(v) = self.statistics_enabled_opt.as_ref() {
            match v {
                parquet_options::StatisticsEnabledOpt::StatisticsEnabled(v) => {
                    struct_ser.serialize_field("statisticsEnabled", v)?;
                }
            }
        }
        if let Some(v) = self.max_statistics_size_opt.as_ref() {
            match v {
                parquet_options::MaxStatisticsSizeOpt::MaxStatisticsSize(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("maxStatisticsSize", ToString::to_string(&v).as_str())?;
                }
            }
        }
        if let Some(v) = self.column_index_truncate_length_opt.as_ref() {
            match v {
                parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("columnIndexTruncateLength", ToString::to_string(&v).as_str())?;
                }
            }
        }
        if let Some(v) = self.encoding_opt.as_ref() {
            match v {
                parquet_options::EncodingOpt::Encoding(v) => {
                    struct_ser.serialize_field("encoding", v)?;
                }
            }
        }
        if let Some(v) = self.bloom_filter_fpp_opt.as_ref() {
            match v {
                parquet_options::BloomFilterFppOpt::BloomFilterFpp(v) => {
                    struct_ser.serialize_field("bloomFilterFpp", v)?;
                }
            }
        }
        if let Some(v) = self.bloom_filter_ndv_opt.as_ref() {
            match v {
                parquet_options::BloomFilterNdvOpt::BloomFilterNdv(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("bloomFilterNdv", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "enable_page_index",
            "enablePageIndex",
            "pruning",
            "skip_metadata",
            "skipMetadata",
            "pushdown_filters",
            "pushdownFilters",
            "reorder_filters",
            "reorderFilters",
            "data_pagesize_limit",
            "dataPagesizeLimit",
            "write_batch_size",
            "writeBatchSize",
            "writer_version",
            "writerVersion",
            "allow_single_file_parallelism",
            "allowSingleFileParallelism",
            "maximum_parallel_row_group_writers",
            "maximumParallelRowGroupWriters",
            "maximum_buffered_record_batches_per_stream",
            "maximumBufferedRecordBatchesPerStream",
            "bloom_filter_on_read",
            "bloomFilterOnRead",
            "bloom_filter_on_write",
            "bloomFilterOnWrite",
            "schema_force_string_view",
            "schemaForceStringView",
            "dictionary_page_size_limit",
            "dictionaryPageSizeLimit",
            "data_page_row_count_limit",
            "dataPageRowCountLimit",
            "max_row_group_size",
            "maxRowGroupSize",
            "created_by",
            "createdBy",
            "metadata_size_hint",
            "metadataSizeHint",
            "compression",
            "dictionary_enabled",
            "dictionaryEnabled",
            "statistics_enabled",
            "statisticsEnabled",
            "max_statistics_size",
            "maxStatisticsSize",
            "column_index_truncate_length",
            "columnIndexTruncateLength",
            "encoding",
            "bloom_filter_fpp",
            "bloomFilterFpp",
            "bloom_filter_ndv",
            "bloomFilterNdv",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            EnablePageIndex,
            Pruning,
            SkipMetadata,
            PushdownFilters,
            ReorderFilters,
            DataPagesizeLimit,
            WriteBatchSize,
            WriterVersion,
            AllowSingleFileParallelism,
            MaximumParallelRowGroupWriters,
            MaximumBufferedRecordBatchesPerStream,
            BloomFilterOnRead,
            BloomFilterOnWrite,
            SchemaForceStringView,
            DictionaryPageSizeLimit,
            DataPageRowCountLimit,
            MaxRowGroupSize,
            CreatedBy,
            MetadataSizeHint,
            Compression,
            DictionaryEnabled,
            StatisticsEnabled,
            MaxStatisticsSize,
            ColumnIndexTruncateLength,
            Encoding,
            BloomFilterFpp,
            BloomFilterNdv,
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
                            "enablePageIndex" | "enable_page_index" => Ok(GeneratedField::EnablePageIndex),
                            "pruning" => Ok(GeneratedField::Pruning),
                            "skipMetadata" | "skip_metadata" => Ok(GeneratedField::SkipMetadata),
                            "pushdownFilters" | "pushdown_filters" => Ok(GeneratedField::PushdownFilters),
                            "reorderFilters" | "reorder_filters" => Ok(GeneratedField::ReorderFilters),
                            "dataPagesizeLimit" | "data_pagesize_limit" => Ok(GeneratedField::DataPagesizeLimit),
                            "writeBatchSize" | "write_batch_size" => Ok(GeneratedField::WriteBatchSize),
                            "writerVersion" | "writer_version" => Ok(GeneratedField::WriterVersion),
                            "allowSingleFileParallelism" | "allow_single_file_parallelism" => Ok(GeneratedField::AllowSingleFileParallelism),
                            "maximumParallelRowGroupWriters" | "maximum_parallel_row_group_writers" => Ok(GeneratedField::MaximumParallelRowGroupWriters),
                            "maximumBufferedRecordBatchesPerStream" | "maximum_buffered_record_batches_per_stream" => Ok(GeneratedField::MaximumBufferedRecordBatchesPerStream),
                            "bloomFilterOnRead" | "bloom_filter_on_read" => Ok(GeneratedField::BloomFilterOnRead),
                            "bloomFilterOnWrite" | "bloom_filter_on_write" => Ok(GeneratedField::BloomFilterOnWrite),
                            "schemaForceStringView" | "schema_force_string_view" => Ok(GeneratedField::SchemaForceStringView),
                            "dictionaryPageSizeLimit" | "dictionary_page_size_limit" => Ok(GeneratedField::DictionaryPageSizeLimit),
                            "dataPageRowCountLimit" | "data_page_row_count_limit" => Ok(GeneratedField::DataPageRowCountLimit),
                            "maxRowGroupSize" | "max_row_group_size" => Ok(GeneratedField::MaxRowGroupSize),
                            "createdBy" | "created_by" => Ok(GeneratedField::CreatedBy),
                            "metadataSizeHint" | "metadata_size_hint" => Ok(GeneratedField::MetadataSizeHint),
                            "compression" => Ok(GeneratedField::Compression),
                            "dictionaryEnabled" | "dictionary_enabled" => Ok(GeneratedField::DictionaryEnabled),
                            "statisticsEnabled" | "statistics_enabled" => Ok(GeneratedField::StatisticsEnabled),
                            "maxStatisticsSize" | "max_statistics_size" => Ok(GeneratedField::MaxStatisticsSize),
                            "columnIndexTruncateLength" | "column_index_truncate_length" => Ok(GeneratedField::ColumnIndexTruncateLength),
                            "encoding" => Ok(GeneratedField::Encoding),
                            "bloomFilterFpp" | "bloom_filter_fpp" => Ok(GeneratedField::BloomFilterFpp),
                            "bloomFilterNdv" | "bloom_filter_ndv" => Ok(GeneratedField::BloomFilterNdv),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ParquetOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ParquetOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut enable_page_index__ = None;
                let mut pruning__ = None;
                let mut skip_metadata__ = None;
                let mut pushdown_filters__ = None;
                let mut reorder_filters__ = None;
                let mut data_pagesize_limit__ = None;
                let mut write_batch_size__ = None;
                let mut writer_version__ = None;
                let mut allow_single_file_parallelism__ = None;
                let mut maximum_parallel_row_group_writers__ = None;
                let mut maximum_buffered_record_batches_per_stream__ = None;
                let mut bloom_filter_on_read__ = None;
                let mut bloom_filter_on_write__ = None;
                let mut schema_force_string_view__ = None;
                let mut dictionary_page_size_limit__ = None;
                let mut data_page_row_count_limit__ = None;
                let mut max_row_group_size__ = None;
                let mut created_by__ = None;
                let mut metadata_size_hint_opt__ = None;
                let mut compression_opt__ = None;
                let mut dictionary_enabled_opt__ = None;
                let mut statistics_enabled_opt__ = None;
                let mut max_statistics_size_opt__ = None;
                let mut column_index_truncate_length_opt__ = None;
                let mut encoding_opt__ = None;
                let mut bloom_filter_fpp_opt__ = None;
                let mut bloom_filter_ndv_opt__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::EnablePageIndex => {
                            if enable_page_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("enablePageIndex"));
                            }
                            enable_page_index__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Pruning => {
                            if pruning__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pruning"));
                            }
                            pruning__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SkipMetadata => {
                            if skip_metadata__.is_some() {
                                return Err(serde::de::Error::duplicate_field("skipMetadata"));
                            }
                            skip_metadata__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PushdownFilters => {
                            if pushdown_filters__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pushdownFilters"));
                            }
                            pushdown_filters__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ReorderFilters => {
                            if reorder_filters__.is_some() {
                                return Err(serde::de::Error::duplicate_field("reorderFilters"));
                            }
                            reorder_filters__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DataPagesizeLimit => {
                            if data_pagesize_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataPagesizeLimit"));
                            }
                            data_pagesize_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::WriteBatchSize => {
                            if write_batch_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writeBatchSize"));
                            }
                            write_batch_size__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::WriterVersion => {
                            if writer_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writerVersion"));
                            }
                            writer_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AllowSingleFileParallelism => {
                            if allow_single_file_parallelism__.is_some() {
                                return Err(serde::de::Error::duplicate_field("allowSingleFileParallelism"));
                            }
                            allow_single_file_parallelism__ = Some(map_.next_value()?);
                        }
                        GeneratedField::MaximumParallelRowGroupWriters => {
                            if maximum_parallel_row_group_writers__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maximumParallelRowGroupWriters"));
                            }
                            maximum_parallel_row_group_writers__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaximumBufferedRecordBatchesPerStream => {
                            if maximum_buffered_record_batches_per_stream__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maximumBufferedRecordBatchesPerStream"));
                            }
                            maximum_buffered_record_batches_per_stream__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::BloomFilterOnRead => {
                            if bloom_filter_on_read__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomFilterOnRead"));
                            }
                            bloom_filter_on_read__ = Some(map_.next_value()?);
                        }
                        GeneratedField::BloomFilterOnWrite => {
                            if bloom_filter_on_write__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomFilterOnWrite"));
                            }
                            bloom_filter_on_write__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SchemaForceStringView => {
                            if schema_force_string_view__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaForceStringView"));
                            }
                            schema_force_string_view__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DictionaryPageSizeLimit => {
                            if dictionary_page_size_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictionaryPageSizeLimit"));
                            }
                            dictionary_page_size_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::DataPageRowCountLimit => {
                            if data_page_row_count_limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataPageRowCountLimit"));
                            }
                            data_page_row_count_limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::MaxRowGroupSize => {
                            if max_row_group_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxRowGroupSize"));
                            }
                            max_row_group_size__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::CreatedBy => {
                            if created_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createdBy"));
                            }
                            created_by__ = Some(map_.next_value()?);
                        }
                        GeneratedField::MetadataSizeHint => {
                            if metadata_size_hint_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("metadataSizeHint"));
                            }
                            metadata_size_hint_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_options::MetadataSizeHintOpt::MetadataSizeHint(x.0));
                        }
                        GeneratedField::Compression => {
                            if compression_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_options::CompressionOpt::Compression);
                        }
                        GeneratedField::DictionaryEnabled => {
                            if dictionary_enabled_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictionaryEnabled"));
                            }
                            dictionary_enabled_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_options::DictionaryEnabledOpt::DictionaryEnabled);
                        }
                        GeneratedField::StatisticsEnabled => {
                            if statistics_enabled_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("statisticsEnabled"));
                            }
                            statistics_enabled_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_options::StatisticsEnabledOpt::StatisticsEnabled);
                        }
                        GeneratedField::MaxStatisticsSize => {
                            if max_statistics_size_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxStatisticsSize"));
                            }
                            max_statistics_size_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_options::MaxStatisticsSizeOpt::MaxStatisticsSize(x.0));
                        }
                        GeneratedField::ColumnIndexTruncateLength => {
                            if column_index_truncate_length_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIndexTruncateLength"));
                            }
                            column_index_truncate_length_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(x.0));
                        }
                        GeneratedField::Encoding => {
                            if encoding_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("encoding"));
                            }
                            encoding_opt__ = map_.next_value::<::std::option::Option<_>>()?.map(parquet_options::EncodingOpt::Encoding);
                        }
                        GeneratedField::BloomFilterFpp => {
                            if bloom_filter_fpp_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomFilterFpp"));
                            }
                            bloom_filter_fpp_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_options::BloomFilterFppOpt::BloomFilterFpp(x.0));
                        }
                        GeneratedField::BloomFilterNdv => {
                            if bloom_filter_ndv_opt__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bloomFilterNdv"));
                            }
                            bloom_filter_ndv_opt__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| parquet_options::BloomFilterNdvOpt::BloomFilterNdv(x.0));
                        }
                    }
                }
                Ok(ParquetOptions {
                    enable_page_index: enable_page_index__.unwrap_or_default(),
                    pruning: pruning__.unwrap_or_default(),
                    skip_metadata: skip_metadata__.unwrap_or_default(),
                    pushdown_filters: pushdown_filters__.unwrap_or_default(),
                    reorder_filters: reorder_filters__.unwrap_or_default(),
                    data_pagesize_limit: data_pagesize_limit__.unwrap_or_default(),
                    write_batch_size: write_batch_size__.unwrap_or_default(),
                    writer_version: writer_version__.unwrap_or_default(),
                    allow_single_file_parallelism: allow_single_file_parallelism__.unwrap_or_default(),
                    maximum_parallel_row_group_writers: maximum_parallel_row_group_writers__.unwrap_or_default(),
                    maximum_buffered_record_batches_per_stream: maximum_buffered_record_batches_per_stream__.unwrap_or_default(),
                    bloom_filter_on_read: bloom_filter_on_read__.unwrap_or_default(),
                    bloom_filter_on_write: bloom_filter_on_write__.unwrap_or_default(),
                    schema_force_string_view: schema_force_string_view__.unwrap_or_default(),
                    dictionary_page_size_limit: dictionary_page_size_limit__.unwrap_or_default(),
                    data_page_row_count_limit: data_page_row_count_limit__.unwrap_or_default(),
                    max_row_group_size: max_row_group_size__.unwrap_or_default(),
                    created_by: created_by__.unwrap_or_default(),
                    metadata_size_hint_opt: metadata_size_hint_opt__,
                    compression_opt: compression_opt__,
                    dictionary_enabled_opt: dictionary_enabled_opt__,
                    statistics_enabled_opt: statistics_enabled_opt__,
                    max_statistics_size_opt: max_statistics_size_opt__,
                    column_index_truncate_length_opt: column_index_truncate_length_opt__,
                    encoding_opt: encoding_opt__,
                    bloom_filter_fpp_opt: bloom_filter_fpp_opt__,
                    bloom_filter_ndv_opt: bloom_filter_ndv_opt__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ParquetOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Precision {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.precision_info != 0 {
            len += 1;
        }
        if self.val.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Precision", len)?;
        if self.precision_info != 0 {
            let v = PrecisionInfo::try_from(self.precision_info)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.precision_info)))?;
            struct_ser.serialize_field("precisionInfo", &v)?;
        }
        if let Some(v) = self.val.as_ref() {
            struct_ser.serialize_field("val", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Precision {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "precision_info",
            "precisionInfo",
            "val",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PrecisionInfo,
            Val,
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
                            "precisionInfo" | "precision_info" => Ok(GeneratedField::PrecisionInfo),
                            "val" => Ok(GeneratedField::Val),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Precision;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Precision")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Precision, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut precision_info__ = None;
                let mut val__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::PrecisionInfo => {
                            if precision_info__.is_some() {
                                return Err(serde::de::Error::duplicate_field("precisionInfo"));
                            }
                            precision_info__ = Some(map_.next_value::<PrecisionInfo>()? as i32);
                        }
                        GeneratedField::Val => {
                            if val__.is_some() {
                                return Err(serde::de::Error::duplicate_field("val"));
                            }
                            val__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Precision {
                    precision_info: precision_info__.unwrap_or_default(),
                    val: val__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Precision", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PrecisionInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Exact => "EXACT",
            Self::Inexact => "INEXACT",
            Self::Absent => "ABSENT",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for PrecisionInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "EXACT",
            "INEXACT",
            "ABSENT",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PrecisionInfo;

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
                    "EXACT" => Ok(PrecisionInfo::Exact),
                    "INEXACT" => Ok(PrecisionInfo::Inexact),
                    "ABSENT" => Ok(PrecisionInfo::Absent),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for PrimaryKeyConstraint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.indices.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.PrimaryKeyConstraint", len)?;
        if !self.indices.is_empty() {
            struct_ser.serialize_field("indices", &self.indices.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PrimaryKeyConstraint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "indices",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Indices,
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
                            "indices" => Ok(GeneratedField::Indices),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PrimaryKeyConstraint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.PrimaryKeyConstraint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PrimaryKeyConstraint, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut indices__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Indices => {
                            if indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("indices"));
                            }
                            indices__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(PrimaryKeyConstraint {
                    indices: indices__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.PrimaryKeyConstraint", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarDictionaryValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index_type.is_some() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarDictionaryValue", len)?;
        if let Some(v) = self.index_type.as_ref() {
            struct_ser.serialize_field("indexType", v)?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarDictionaryValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index_type",
            "indexType",
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IndexType,
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
                            "indexType" | "index_type" => Ok(GeneratedField::IndexType),
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
            type Value = ScalarDictionaryValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarDictionaryValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarDictionaryValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index_type__ = None;
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::IndexType => {
                            if index_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("indexType"));
                            }
                            index_type__ = map_.next_value()?;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ScalarDictionaryValue {
                    index_type: index_type__,
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarDictionaryValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarFixedSizeBinary {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.values.is_empty() {
            len += 1;
        }
        if self.length != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarFixedSizeBinary", len)?;
        if !self.values.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("values", pbjson::private::base64::encode(&self.values).as_str())?;
        }
        if self.length != 0 {
            struct_ser.serialize_field("length", &self.length)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarFixedSizeBinary {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "values",
            "length",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Values,
            Length,
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
                            "values" => Ok(GeneratedField::Values),
                            "length" => Ok(GeneratedField::Length),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarFixedSizeBinary;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarFixedSizeBinary")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarFixedSizeBinary, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut values__ = None;
                let mut length__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Length => {
                            if length__.is_some() {
                                return Err(serde::de::Error::duplicate_field("length"));
                            }
                            length__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ScalarFixedSizeBinary {
                    values: values__.unwrap_or_default(),
                    length: length__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarFixedSizeBinary", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarNestedValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ipc_message.is_empty() {
            len += 1;
        }
        if !self.arrow_data.is_empty() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.dictionaries.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarNestedValue", len)?;
        if !self.ipc_message.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("ipcMessage", pbjson::private::base64::encode(&self.ipc_message).as_str())?;
        }
        if !self.arrow_data.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("arrowData", pbjson::private::base64::encode(&self.arrow_data).as_str())?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.dictionaries.is_empty() {
            struct_ser.serialize_field("dictionaries", &self.dictionaries)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarNestedValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ipc_message",
            "ipcMessage",
            "arrow_data",
            "arrowData",
            "schema",
            "dictionaries",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IpcMessage,
            ArrowData,
            Schema,
            Dictionaries,
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
                            "ipcMessage" | "ipc_message" => Ok(GeneratedField::IpcMessage),
                            "arrowData" | "arrow_data" => Ok(GeneratedField::ArrowData),
                            "schema" => Ok(GeneratedField::Schema),
                            "dictionaries" => Ok(GeneratedField::Dictionaries),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarNestedValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarNestedValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarNestedValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ipc_message__ = None;
                let mut arrow_data__ = None;
                let mut schema__ = None;
                let mut dictionaries__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::IpcMessage => {
                            if ipc_message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ipcMessage"));
                            }
                            ipc_message__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ArrowData => {
                            if arrow_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowData"));
                            }
                            arrow_data__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::Dictionaries => {
                            if dictionaries__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictionaries"));
                            }
                            dictionaries__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ScalarNestedValue {
                    ipc_message: ipc_message__.unwrap_or_default(),
                    arrow_data: arrow_data__.unwrap_or_default(),
                    schema: schema__,
                    dictionaries: dictionaries__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarNestedValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for scalar_nested_value::Dictionary {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.ipc_message.is_empty() {
            len += 1;
        }
        if !self.arrow_data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarNestedValue.Dictionary", len)?;
        if !self.ipc_message.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("ipcMessage", pbjson::private::base64::encode(&self.ipc_message).as_str())?;
        }
        if !self.arrow_data.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("arrowData", pbjson::private::base64::encode(&self.arrow_data).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for scalar_nested_value::Dictionary {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ipc_message",
            "ipcMessage",
            "arrow_data",
            "arrowData",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IpcMessage,
            ArrowData,
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
                            "ipcMessage" | "ipc_message" => Ok(GeneratedField::IpcMessage),
                            "arrowData" | "arrow_data" => Ok(GeneratedField::ArrowData),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = scalar_nested_value::Dictionary;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarNestedValue.Dictionary")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<scalar_nested_value::Dictionary, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut ipc_message__ = None;
                let mut arrow_data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::IpcMessage => {
                            if ipc_message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ipcMessage"));
                            }
                            ipc_message__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ArrowData => {
                            if arrow_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowData"));
                            }
                            arrow_data__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(scalar_nested_value::Dictionary {
                    ipc_message: ipc_message__.unwrap_or_default(),
                    arrow_data: arrow_data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarNestedValue.Dictionary", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarTime32Value {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarTime32Value", len)?;
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_time32_value::Value::Time32SecondValue(v) => {
                    struct_ser.serialize_field("time32SecondValue", v)?;
                }
                scalar_time32_value::Value::Time32MillisecondValue(v) => {
                    struct_ser.serialize_field("time32MillisecondValue", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarTime32Value {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "time32_second_value",
            "time32SecondValue",
            "time32_millisecond_value",
            "time32MillisecondValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Time32SecondValue,
            Time32MillisecondValue,
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
                            "time32SecondValue" | "time32_second_value" => Ok(GeneratedField::Time32SecondValue),
                            "time32MillisecondValue" | "time32_millisecond_value" => Ok(GeneratedField::Time32MillisecondValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarTime32Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarTime32Value")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarTime32Value, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Time32SecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time32SecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time32_value::Value::Time32SecondValue(x.0));
                        }
                        GeneratedField::Time32MillisecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time32MillisecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time32_value::Value::Time32MillisecondValue(x.0));
                        }
                    }
                }
                Ok(ScalarTime32Value {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarTime32Value", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarTime64Value {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarTime64Value", len)?;
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_time64_value::Value::Time64MicrosecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("time64MicrosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_time64_value::Value::Time64NanosecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("time64NanosecondValue", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarTime64Value {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "time64_microsecond_value",
            "time64MicrosecondValue",
            "time64_nanosecond_value",
            "time64NanosecondValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Time64MicrosecondValue,
            Time64NanosecondValue,
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
                            "time64MicrosecondValue" | "time64_microsecond_value" => Ok(GeneratedField::Time64MicrosecondValue),
                            "time64NanosecondValue" | "time64_nanosecond_value" => Ok(GeneratedField::Time64NanosecondValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarTime64Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarTime64Value")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarTime64Value, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Time64MicrosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time64MicrosecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time64_value::Value::Time64MicrosecondValue(x.0));
                        }
                        GeneratedField::Time64NanosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time64NanosecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_time64_value::Value::Time64NanosecondValue(x.0));
                        }
                    }
                }
                Ok(ScalarTime64Value {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarTime64Value", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarTimestampValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.timezone.is_empty() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarTimestampValue", len)?;
        if !self.timezone.is_empty() {
            struct_ser.serialize_field("timezone", &self.timezone)?;
        }
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_timestamp_value::Value::TimeMicrosecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("timeMicrosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_timestamp_value::Value::TimeNanosecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("timeNanosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_timestamp_value::Value::TimeSecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("timeSecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_timestamp_value::Value::TimeMillisecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("timeMillisecondValue", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarTimestampValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "timezone",
            "time_microsecond_value",
            "timeMicrosecondValue",
            "time_nanosecond_value",
            "timeNanosecondValue",
            "time_second_value",
            "timeSecondValue",
            "time_millisecond_value",
            "timeMillisecondValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Timezone,
            TimeMicrosecondValue,
            TimeNanosecondValue,
            TimeSecondValue,
            TimeMillisecondValue,
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
                            "timezone" => Ok(GeneratedField::Timezone),
                            "timeMicrosecondValue" | "time_microsecond_value" => Ok(GeneratedField::TimeMicrosecondValue),
                            "timeNanosecondValue" | "time_nanosecond_value" => Ok(GeneratedField::TimeNanosecondValue),
                            "timeSecondValue" | "time_second_value" => Ok(GeneratedField::TimeSecondValue),
                            "timeMillisecondValue" | "time_millisecond_value" => Ok(GeneratedField::TimeMillisecondValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarTimestampValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarTimestampValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarTimestampValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut timezone__ = None;
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Timezone => {
                            if timezone__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timezone"));
                            }
                            timezone__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TimeMicrosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeMicrosecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeMicrosecondValue(x.0));
                        }
                        GeneratedField::TimeNanosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeNanosecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeNanosecondValue(x.0));
                        }
                        GeneratedField::TimeSecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeSecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeSecondValue(x.0));
                        }
                        GeneratedField::TimeMillisecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeMillisecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_timestamp_value::Value::TimeMillisecondValue(x.0));
                        }
                    }
                }
                Ok(ScalarTimestampValue {
                    timezone: timezone__.unwrap_or_default(),
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarTimestampValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.ScalarValue", len)?;
        if let Some(v) = self.value.as_ref() {
            match v {
                scalar_value::Value::NullValue(v) => {
                    struct_ser.serialize_field("nullValue", v)?;
                }
                scalar_value::Value::BoolValue(v) => {
                    struct_ser.serialize_field("boolValue", v)?;
                }
                scalar_value::Value::Utf8Value(v) => {
                    struct_ser.serialize_field("utf8Value", v)?;
                }
                scalar_value::Value::LargeUtf8Value(v) => {
                    struct_ser.serialize_field("largeUtf8Value", v)?;
                }
                scalar_value::Value::Utf8ViewValue(v) => {
                    struct_ser.serialize_field("utf8ViewValue", v)?;
                }
                scalar_value::Value::Int8Value(v) => {
                    struct_ser.serialize_field("int8Value", v)?;
                }
                scalar_value::Value::Int16Value(v) => {
                    struct_ser.serialize_field("int16Value", v)?;
                }
                scalar_value::Value::Int32Value(v) => {
                    struct_ser.serialize_field("int32Value", v)?;
                }
                scalar_value::Value::Int64Value(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("int64Value", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::Uint8Value(v) => {
                    struct_ser.serialize_field("uint8Value", v)?;
                }
                scalar_value::Value::Uint16Value(v) => {
                    struct_ser.serialize_field("uint16Value", v)?;
                }
                scalar_value::Value::Uint32Value(v) => {
                    struct_ser.serialize_field("uint32Value", v)?;
                }
                scalar_value::Value::Uint64Value(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("uint64Value", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::Float32Value(v) => {
                    struct_ser.serialize_field("float32Value", v)?;
                }
                scalar_value::Value::Float64Value(v) => {
                    struct_ser.serialize_field("float64Value", v)?;
                }
                scalar_value::Value::Date32Value(v) => {
                    struct_ser.serialize_field("date32Value", v)?;
                }
                scalar_value::Value::Time32Value(v) => {
                    struct_ser.serialize_field("time32Value", v)?;
                }
                scalar_value::Value::LargeListValue(v) => {
                    struct_ser.serialize_field("largeListValue", v)?;
                }
                scalar_value::Value::ListValue(v) => {
                    struct_ser.serialize_field("listValue", v)?;
                }
                scalar_value::Value::FixedSizeListValue(v) => {
                    struct_ser.serialize_field("fixedSizeListValue", v)?;
                }
                scalar_value::Value::StructValue(v) => {
                    struct_ser.serialize_field("structValue", v)?;
                }
                scalar_value::Value::MapValue(v) => {
                    struct_ser.serialize_field("mapValue", v)?;
                }
                scalar_value::Value::Decimal128Value(v) => {
                    struct_ser.serialize_field("decimal128Value", v)?;
                }
                scalar_value::Value::Decimal256Value(v) => {
                    struct_ser.serialize_field("decimal256Value", v)?;
                }
                scalar_value::Value::Date64Value(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("date64Value", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::IntervalYearmonthValue(v) => {
                    struct_ser.serialize_field("intervalYearmonthValue", v)?;
                }
                scalar_value::Value::DurationSecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("durationSecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::DurationMillisecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("durationMillisecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::DurationMicrosecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("durationMicrosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::DurationNanosecondValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("durationNanosecondValue", ToString::to_string(&v).as_str())?;
                }
                scalar_value::Value::TimestampValue(v) => {
                    struct_ser.serialize_field("timestampValue", v)?;
                }
                scalar_value::Value::DictionaryValue(v) => {
                    struct_ser.serialize_field("dictionaryValue", v)?;
                }
                scalar_value::Value::BinaryValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("binaryValue", pbjson::private::base64::encode(&v).as_str())?;
                }
                scalar_value::Value::LargeBinaryValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("largeBinaryValue", pbjson::private::base64::encode(&v).as_str())?;
                }
                scalar_value::Value::BinaryViewValue(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("binaryViewValue", pbjson::private::base64::encode(&v).as_str())?;
                }
                scalar_value::Value::Time64Value(v) => {
                    struct_ser.serialize_field("time64Value", v)?;
                }
                scalar_value::Value::IntervalDaytimeValue(v) => {
                    struct_ser.serialize_field("intervalDaytimeValue", v)?;
                }
                scalar_value::Value::IntervalMonthDayNano(v) => {
                    struct_ser.serialize_field("intervalMonthDayNano", v)?;
                }
                scalar_value::Value::FixedSizeBinaryValue(v) => {
                    struct_ser.serialize_field("fixedSizeBinaryValue", v)?;
                }
                scalar_value::Value::UnionValue(v) => {
                    struct_ser.serialize_field("unionValue", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "null_value",
            "nullValue",
            "bool_value",
            "boolValue",
            "utf8_value",
            "utf8Value",
            "large_utf8_value",
            "largeUtf8Value",
            "utf8_view_value",
            "utf8ViewValue",
            "int8_value",
            "int8Value",
            "int16_value",
            "int16Value",
            "int32_value",
            "int32Value",
            "int64_value",
            "int64Value",
            "uint8_value",
            "uint8Value",
            "uint16_value",
            "uint16Value",
            "uint32_value",
            "uint32Value",
            "uint64_value",
            "uint64Value",
            "float32_value",
            "float32Value",
            "float64_value",
            "float64Value",
            "date_32_value",
            "date32Value",
            "time32_value",
            "time32Value",
            "large_list_value",
            "largeListValue",
            "list_value",
            "listValue",
            "fixed_size_list_value",
            "fixedSizeListValue",
            "struct_value",
            "structValue",
            "map_value",
            "mapValue",
            "decimal128_value",
            "decimal128Value",
            "decimal256_value",
            "decimal256Value",
            "date_64_value",
            "date64Value",
            "interval_yearmonth_value",
            "intervalYearmonthValue",
            "duration_second_value",
            "durationSecondValue",
            "duration_millisecond_value",
            "durationMillisecondValue",
            "duration_microsecond_value",
            "durationMicrosecondValue",
            "duration_nanosecond_value",
            "durationNanosecondValue",
            "timestamp_value",
            "timestampValue",
            "dictionary_value",
            "dictionaryValue",
            "binary_value",
            "binaryValue",
            "large_binary_value",
            "largeBinaryValue",
            "binary_view_value",
            "binaryViewValue",
            "time64_value",
            "time64Value",
            "interval_daytime_value",
            "intervalDaytimeValue",
            "interval_month_day_nano",
            "intervalMonthDayNano",
            "fixed_size_binary_value",
            "fixedSizeBinaryValue",
            "union_value",
            "unionValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NullValue,
            BoolValue,
            Utf8Value,
            LargeUtf8Value,
            Utf8ViewValue,
            Int8Value,
            Int16Value,
            Int32Value,
            Int64Value,
            Uint8Value,
            Uint16Value,
            Uint32Value,
            Uint64Value,
            Float32Value,
            Float64Value,
            Date32Value,
            Time32Value,
            LargeListValue,
            ListValue,
            FixedSizeListValue,
            StructValue,
            MapValue,
            Decimal128Value,
            Decimal256Value,
            Date64Value,
            IntervalYearmonthValue,
            DurationSecondValue,
            DurationMillisecondValue,
            DurationMicrosecondValue,
            DurationNanosecondValue,
            TimestampValue,
            DictionaryValue,
            BinaryValue,
            LargeBinaryValue,
            BinaryViewValue,
            Time64Value,
            IntervalDaytimeValue,
            IntervalMonthDayNano,
            FixedSizeBinaryValue,
            UnionValue,
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
                            "nullValue" | "null_value" => Ok(GeneratedField::NullValue),
                            "boolValue" | "bool_value" => Ok(GeneratedField::BoolValue),
                            "utf8Value" | "utf8_value" => Ok(GeneratedField::Utf8Value),
                            "largeUtf8Value" | "large_utf8_value" => Ok(GeneratedField::LargeUtf8Value),
                            "utf8ViewValue" | "utf8_view_value" => Ok(GeneratedField::Utf8ViewValue),
                            "int8Value" | "int8_value" => Ok(GeneratedField::Int8Value),
                            "int16Value" | "int16_value" => Ok(GeneratedField::Int16Value),
                            "int32Value" | "int32_value" => Ok(GeneratedField::Int32Value),
                            "int64Value" | "int64_value" => Ok(GeneratedField::Int64Value),
                            "uint8Value" | "uint8_value" => Ok(GeneratedField::Uint8Value),
                            "uint16Value" | "uint16_value" => Ok(GeneratedField::Uint16Value),
                            "uint32Value" | "uint32_value" => Ok(GeneratedField::Uint32Value),
                            "uint64Value" | "uint64_value" => Ok(GeneratedField::Uint64Value),
                            "float32Value" | "float32_value" => Ok(GeneratedField::Float32Value),
                            "float64Value" | "float64_value" => Ok(GeneratedField::Float64Value),
                            "date32Value" | "date_32_value" => Ok(GeneratedField::Date32Value),
                            "time32Value" | "time32_value" => Ok(GeneratedField::Time32Value),
                            "largeListValue" | "large_list_value" => Ok(GeneratedField::LargeListValue),
                            "listValue" | "list_value" => Ok(GeneratedField::ListValue),
                            "fixedSizeListValue" | "fixed_size_list_value" => Ok(GeneratedField::FixedSizeListValue),
                            "structValue" | "struct_value" => Ok(GeneratedField::StructValue),
                            "mapValue" | "map_value" => Ok(GeneratedField::MapValue),
                            "decimal128Value" | "decimal128_value" => Ok(GeneratedField::Decimal128Value),
                            "decimal256Value" | "decimal256_value" => Ok(GeneratedField::Decimal256Value),
                            "date64Value" | "date_64_value" => Ok(GeneratedField::Date64Value),
                            "intervalYearmonthValue" | "interval_yearmonth_value" => Ok(GeneratedField::IntervalYearmonthValue),
                            "durationSecondValue" | "duration_second_value" => Ok(GeneratedField::DurationSecondValue),
                            "durationMillisecondValue" | "duration_millisecond_value" => Ok(GeneratedField::DurationMillisecondValue),
                            "durationMicrosecondValue" | "duration_microsecond_value" => Ok(GeneratedField::DurationMicrosecondValue),
                            "durationNanosecondValue" | "duration_nanosecond_value" => Ok(GeneratedField::DurationNanosecondValue),
                            "timestampValue" | "timestamp_value" => Ok(GeneratedField::TimestampValue),
                            "dictionaryValue" | "dictionary_value" => Ok(GeneratedField::DictionaryValue),
                            "binaryValue" | "binary_value" => Ok(GeneratedField::BinaryValue),
                            "largeBinaryValue" | "large_binary_value" => Ok(GeneratedField::LargeBinaryValue),
                            "binaryViewValue" | "binary_view_value" => Ok(GeneratedField::BinaryViewValue),
                            "time64Value" | "time64_value" => Ok(GeneratedField::Time64Value),
                            "intervalDaytimeValue" | "interval_daytime_value" => Ok(GeneratedField::IntervalDaytimeValue),
                            "intervalMonthDayNano" | "interval_month_day_nano" => Ok(GeneratedField::IntervalMonthDayNano),
                            "fixedSizeBinaryValue" | "fixed_size_binary_value" => Ok(GeneratedField::FixedSizeBinaryValue),
                            "unionValue" | "union_value" => Ok(GeneratedField::UnionValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.ScalarValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NullValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::NullValue)
;
                        }
                        GeneratedField::BoolValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("boolValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::BoolValue);
                        }
                        GeneratedField::Utf8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("utf8Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Utf8Value);
                        }
                        GeneratedField::LargeUtf8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("largeUtf8Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::LargeUtf8Value);
                        }
                        GeneratedField::Utf8ViewValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("utf8ViewValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Utf8ViewValue);
                        }
                        GeneratedField::Int8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int8Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int8Value(x.0));
                        }
                        GeneratedField::Int16Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int16Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int16Value(x.0));
                        }
                        GeneratedField::Int32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int32Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int32Value(x.0));
                        }
                        GeneratedField::Int64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("int64Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Int64Value(x.0));
                        }
                        GeneratedField::Uint8Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint8Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint8Value(x.0));
                        }
                        GeneratedField::Uint16Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint16Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint16Value(x.0));
                        }
                        GeneratedField::Uint32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint32Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint32Value(x.0));
                        }
                        GeneratedField::Uint64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("uint64Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Uint64Value(x.0));
                        }
                        GeneratedField::Float32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("float32Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Float32Value(x.0));
                        }
                        GeneratedField::Float64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("float64Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Float64Value(x.0));
                        }
                        GeneratedField::Date32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("date32Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Date32Value(x.0));
                        }
                        GeneratedField::Time32Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time32Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Time32Value)
;
                        }
                        GeneratedField::LargeListValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("largeListValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::LargeListValue)
;
                        }
                        GeneratedField::ListValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::ListValue)
;
                        }
                        GeneratedField::FixedSizeListValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fixedSizeListValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::FixedSizeListValue)
;
                        }
                        GeneratedField::StructValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("structValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::StructValue)
;
                        }
                        GeneratedField::MapValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mapValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::MapValue)
;
                        }
                        GeneratedField::Decimal128Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("decimal128Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Decimal128Value)
;
                        }
                        GeneratedField::Decimal256Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("decimal256Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Decimal256Value)
;
                        }
                        GeneratedField::Date64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("date64Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::Date64Value(x.0));
                        }
                        GeneratedField::IntervalYearmonthValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalYearmonthValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::IntervalYearmonthValue(x.0));
                        }
                        GeneratedField::DurationSecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durationSecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::DurationSecondValue(x.0));
                        }
                        GeneratedField::DurationMillisecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durationMillisecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::DurationMillisecondValue(x.0));
                        }
                        GeneratedField::DurationMicrosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durationMicrosecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::DurationMicrosecondValue(x.0));
                        }
                        GeneratedField::DurationNanosecondValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("durationNanosecondValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| scalar_value::Value::DurationNanosecondValue(x.0));
                        }
                        GeneratedField::TimestampValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestampValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::TimestampValue)
;
                        }
                        GeneratedField::DictionaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dictionaryValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::DictionaryValue)
;
                        }
                        GeneratedField::BinaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| scalar_value::Value::BinaryValue(x.0));
                        }
                        GeneratedField::LargeBinaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("largeBinaryValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| scalar_value::Value::LargeBinaryValue(x.0));
                        }
                        GeneratedField::BinaryViewValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryViewValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| scalar_value::Value::BinaryViewValue(x.0));
                        }
                        GeneratedField::Time64Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time64Value"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::Time64Value)
;
                        }
                        GeneratedField::IntervalDaytimeValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalDaytimeValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::IntervalDaytimeValue)
;
                        }
                        GeneratedField::IntervalMonthDayNano => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalMonthDayNano"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::IntervalMonthDayNano)
;
                        }
                        GeneratedField::FixedSizeBinaryValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fixedSizeBinaryValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::FixedSizeBinaryValue)
;
                        }
                        GeneratedField::UnionValue => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unionValue"));
                            }
                            value__ = map_.next_value::<::std::option::Option<_>>()?.map(scalar_value::Value::UnionValue)
;
                        }
                    }
                }
                Ok(ScalarValue {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.ScalarValue", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for Statistics {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.num_rows.is_some() {
            len += 1;
        }
        if self.total_byte_size.is_some() {
            len += 1;
        }
        if !self.column_stats.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.Statistics", len)?;
        if let Some(v) = self.num_rows.as_ref() {
            struct_ser.serialize_field("numRows", v)?;
        }
        if let Some(v) = self.total_byte_size.as_ref() {
            struct_ser.serialize_field("totalByteSize", v)?;
        }
        if !self.column_stats.is_empty() {
            struct_ser.serialize_field("columnStats", &self.column_stats)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Statistics {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "num_rows",
            "numRows",
            "total_byte_size",
            "totalByteSize",
            "column_stats",
            "columnStats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NumRows,
            TotalByteSize,
            ColumnStats,
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
                            "numRows" | "num_rows" => Ok(GeneratedField::NumRows),
                            "totalByteSize" | "total_byte_size" => Ok(GeneratedField::TotalByteSize),
                            "columnStats" | "column_stats" => Ok(GeneratedField::ColumnStats),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Statistics;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.Statistics")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Statistics, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut num_rows__ = None;
                let mut total_byte_size__ = None;
                let mut column_stats__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NumRows => {
                            if num_rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numRows"));
                            }
                            num_rows__ = map_.next_value()?;
                        }
                        GeneratedField::TotalByteSize => {
                            if total_byte_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("totalByteSize"));
                            }
                            total_byte_size__ = map_.next_value()?;
                        }
                        GeneratedField::ColumnStats => {
                            if column_stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnStats"));
                            }
                            column_stats__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Statistics {
                    num_rows: num_rows__,
                    total_byte_size: total_byte_size__,
                    column_stats: column_stats__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.Statistics", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for TableParquetOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.global.is_some() {
            len += 1;
        }
        if !self.column_specific_options.is_empty() {
            len += 1;
        }
        if !self.key_value_metadata.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.TableParquetOptions", len)?;
        if let Some(v) = self.global.as_ref() {
            struct_ser.serialize_field("global", v)?;
        }
        if !self.column_specific_options.is_empty() {
            struct_ser.serialize_field("columnSpecificOptions", &self.column_specific_options)?;
        }
        if !self.key_value_metadata.is_empty() {
            struct_ser.serialize_field("keyValueMetadata", &self.key_value_metadata)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableParquetOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "global",
            "column_specific_options",
            "columnSpecificOptions",
            "key_value_metadata",
            "keyValueMetadata",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Global,
            ColumnSpecificOptions,
            KeyValueMetadata,
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
                            "global" => Ok(GeneratedField::Global),
                            "columnSpecificOptions" | "column_specific_options" => Ok(GeneratedField::ColumnSpecificOptions),
                            "keyValueMetadata" | "key_value_metadata" => Ok(GeneratedField::KeyValueMetadata),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableParquetOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.TableParquetOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TableParquetOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut global__ = None;
                let mut column_specific_options__ = None;
                let mut key_value_metadata__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Global => {
                            if global__.is_some() {
                                return Err(serde::de::Error::duplicate_field("global"));
                            }
                            global__ = map_.next_value()?;
                        }
                        GeneratedField::ColumnSpecificOptions => {
                            if column_specific_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnSpecificOptions"));
                            }
                            column_specific_options__ = Some(map_.next_value()?);
                        }
                        GeneratedField::KeyValueMetadata => {
                            if key_value_metadata__.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyValueMetadata"));
                            }
                            key_value_metadata__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(TableParquetOptions {
                    global: global__,
                    column_specific_options: column_specific_options__.unwrap_or_default(),
                    key_value_metadata: key_value_metadata__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.TableParquetOptions", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for UnionField {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.field_id != 0 {
            len += 1;
        }
        if self.field.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.UnionField", len)?;
        if self.field_id != 0 {
            struct_ser.serialize_field("fieldId", &self.field_id)?;
        }
        if let Some(v) = self.field.as_ref() {
            struct_ser.serialize_field("field", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnionField {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "field_id",
            "fieldId",
            "field",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FieldId,
            Field,
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
                            "fieldId" | "field_id" => Ok(GeneratedField::FieldId),
                            "field" => Ok(GeneratedField::Field),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.UnionField")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnionField, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut field_id__ = None;
                let mut field__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FieldId => {
                            if field_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldId"));
                            }
                            field_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Field => {
                            if field__.is_some() {
                                return Err(serde::de::Error::duplicate_field("field"));
                            }
                            field__ = map_.next_value()?;
                        }
                    }
                }
                Ok(UnionField {
                    field_id: field_id__.unwrap_or_default(),
                    field: field__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.UnionField", FIELDS, GeneratedVisitor)
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
impl serde::Serialize for UnionValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value_id != 0 {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        if !self.fields.is_empty() {
            len += 1;
        }
        if self.mode != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.UnionValue", len)?;
        if self.value_id != 0 {
            struct_ser.serialize_field("valueId", &self.value_id)?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        if !self.fields.is_empty() {
            struct_ser.serialize_field("fields", &self.fields)?;
        }
        if self.mode != 0 {
            let v = UnionMode::try_from(self.mode)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.mode)))?;
            struct_ser.serialize_field("mode", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnionValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "value_id",
            "valueId",
            "value",
            "fields",
            "mode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ValueId,
            Value,
            Fields,
            Mode,
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
                            "valueId" | "value_id" => Ok(GeneratedField::ValueId),
                            "value" => Ok(GeneratedField::Value),
                            "fields" => Ok(GeneratedField::Fields),
                            "mode" => Ok(GeneratedField::Mode),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.UnionValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnionValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value_id__ = None;
                let mut value__ = None;
                let mut fields__ = None;
                let mut mode__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ValueId => {
                            if value_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valueId"));
                            }
                            value_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map_.next_value()?;
                        }
                        GeneratedField::Fields => {
                            if fields__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Mode => {
                            if mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mode"));
                            }
                            mode__ = Some(map_.next_value::<UnionMode>()? as i32);
                        }
                    }
                }
                Ok(UnionValue {
                    value_id: value_id__.unwrap_or_default(),
                    value: value__,
                    fields: fields__.unwrap_or_default(),
                    mode: mode__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.UnionValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UniqueConstraint {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.indices.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion_common.UniqueConstraint", len)?;
        if !self.indices.is_empty() {
            struct_ser.serialize_field("indices", &self.indices.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UniqueConstraint {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "indices",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Indices,
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
                            "indices" => Ok(GeneratedField::Indices),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UniqueConstraint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion_common.UniqueConstraint")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UniqueConstraint, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut indices__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Indices => {
                            if indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("indices"));
                            }
                            indices__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(UniqueConstraint {
                    indices: indices__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion_common.UniqueConstraint", FIELDS, GeneratedVisitor)
    }
}
