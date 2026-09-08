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

use super::SubstraitConsumer;
use super::utils::{DEFAULT_TIMEZONE, from_substrait_precision, next_struct_field_name};
#[expect(deprecated)]
use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF,
    DECIMAL_128_TYPE_VARIATION_REF, DECIMAL_256_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_INTERVAL_DAY_TYPE_VARIATION_REF,
    DEFAULT_MAP_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    DICTIONARY_MAP_TYPE_VARIATION_REF, DURATION_INTERVAL_DAY_TYPE_VARIATION_REF,
    INTERVAL_DAY_TIME_TYPE_REF, INTERVAL_MONTH_DAY_NANO_TYPE_NAME,
    INTERVAL_MONTH_DAY_NANO_TYPE_REF, INTERVAL_YEAR_MONTH_TYPE_REF,
    LARGE_CONTAINER_TYPE_VARIATION_REF, TIME_32_TYPE_VARIATION_REF,
    TIME_64_TYPE_VARIATION_REF, TIMESTAMP_MICRO_TYPE_VARIATION_REF,
    TIMESTAMP_MILLI_TYPE_VARIATION_REF, TIMESTAMP_NANO_TYPE_VARIATION_REF,
    TIMESTAMP_SECOND_TYPE_VARIATION_REF, UNSIGNED_INTEGER_TYPE_VARIATION_REF,
    VIEW_CONTAINER_TYPE_VARIATION_REF,
};
use crate::variation_const::{FLOAT_16_TYPE_NAME, NULL_TYPE_NAME};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit,
};
use datafusion::common::{
    DFSchema, not_impl_err, substrait_datafusion_err, substrait_err,
};
use std::sync::Arc;
use substrait::proto::{NamedStruct, Type, r#type};

pub(crate) fn field_from_substrait_type_without_names(
    consumer: &impl SubstraitConsumer,
    dt: &Type,
) -> datafusion::common::Result<FieldRef> {
    let field = substrait_type_to_field(consumer, dt, &[], &mut 0)?;
    Ok(Arc::new(field))
}

pub(crate) fn from_substrait_type_without_names(
    consumer: &impl SubstraitConsumer,
    dt: &Type,
) -> datafusion::common::Result<DataType> {
    from_substrait_type(consumer, dt, &[], &mut 0)
}

pub fn field_from_substrait_type(
    consumer: &impl SubstraitConsumer,
    dt: &Type,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> datafusion::common::Result<FieldRef> {
    // We could add nullability here now that we are returning a Field
    let field = substrait_type_to_field(consumer, dt, dfs_names, name_idx)?;
    Ok(Arc::new(field))
}

/// Converts a Substrait type to an Arrow [`Field`] and attaches the metadata
/// that [`SubstraitConsumer::consume_type_metadata`] supplies for it.
///
/// This is the only place that attaches type metadata. Every Arrow field built
/// from a Substrait type goes through here, including the fields of a nested
/// list, map, or struct, so a nested type keeps its metadata without each call
/// site having to ask for it.
///
/// The field is named `""` and is nullable. Callers set the name and the
/// nullability that they need.
pub(crate) fn substrait_type_to_field(
    consumer: &impl SubstraitConsumer,
    dt: &Type,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> datafusion::common::Result<Field> {
    let data_type = from_substrait_type(consumer, dt, dfs_names, name_idx)?;
    let field = Field::new("", data_type, true);
    match consumer.consume_type_metadata(dt)? {
        Some(metadata) => Ok(field.with_metadata(metadata)),
        None => Ok(field),
    }
}

pub fn from_substrait_type(
    consumer: &impl SubstraitConsumer,
    dt: &Type,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> datafusion::common::Result<DataType> {
    match &dt.kind {
        Some(s_kind) => match s_kind {
            r#type::Kind::Bool(_) => Ok(DataType::Boolean),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int8),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt8),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int16),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt16),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int32),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt32),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int64),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(DataType::Float32),
            r#type::Kind::Fp64(_) => Ok(DataType::Float64),
            #[expect(deprecated)]
            r#type::Kind::Timestamp(ts) => {
                // Kept for backwards compatibility, new plans should use PrecisionTimestamp(Tz) instead
                #[expect(deprecated)]
                match ts.type_variation_reference {
                    TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Second, None))
                    }
                    TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
                    }
                    TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                    TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
                    }
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    ),
                }
            }
            r#type::Kind::PrecisionTimestamp(pts) => {
                let unit = from_substrait_precision(pts.precision, "PrecisionTimestamp")?;
                Ok(DataType::Timestamp(unit, None))
            }
            r#type::Kind::PrecisionTimestampTz(pts) => {
                let unit =
                    from_substrait_precision(pts.precision, "PrecisionTimestampTz")?;
                Ok(DataType::Timestamp(unit, Some(DEFAULT_TIMEZONE.into())))
            }
            r#type::Kind::PrecisionTime(pt) => {
                let time_unit = from_substrait_precision(pt.precision, "PrecisionTime")?;
                match pt.type_variation_reference {
                    TIME_32_TYPE_VARIATION_REF => Ok(DataType::Time32(time_unit)),
                    TIME_64_TYPE_VARIATION_REF => Ok(DataType::Time64(time_unit)),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    ),
                }
            }
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF => Ok(DataType::Date32),
                DATE_64_TYPE_VARIATION_REF => Ok(DataType::Date64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Binary),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeBinary),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::BinaryView),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::FixedBinary(fixed) => {
                Ok(DataType::FixedSizeBinary(fixed.length))
            }
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeUtf8),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8View),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::List(list) => {
                let inner_type = list.r#type.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("List type must have inner type")
                })?;
                // `substrait_type_to_field` always returns a nullable field, so
                // we ignore Substrait's nullability here to match
                // to_substrait_literal, which always creates nullable lists
                let field = Arc::new(
                    substrait_type_to_field(consumer, inner_type, dfs_names, name_idx)?
                        .with_name(Field::LIST_FIELD_DEFAULT_NAME),
                );
                match list.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::List(field)),
                    LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeList(field)),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    )?,
                }
            }
            r#type::Kind::Map(map) => {
                let key_type = map.key.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have key type")
                })?;
                let value_type = map.value.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have value type")
                })?;
                let key_field =
                    substrait_type_to_field(consumer, key_type, dfs_names, name_idx)?;
                let value_field =
                    substrait_type_to_field(consumer, value_type, dfs_names, name_idx)?;

                match map.type_variation_reference {
                    DEFAULT_MAP_TYPE_VARIATION_REF => {
                        let key_field =
                            Arc::new(key_field.with_name("key").with_nullable(false));
                        let value_field = Arc::new(value_field.with_name("value"));
                        Ok(DataType::Map(
                            Arc::new(Field::new_struct(
                                "entries",
                                [key_field, value_field],
                                false, // The inner map field is always non-nullable (Arrow #1697),
                            )),
                            false, // whether keys are sorted
                        ))
                    }
                    // A Dictionary holds bare data types, so it cannot carry the
                    // metadata of its key and value types.
                    DICTIONARY_MAP_TYPE_VARIATION_REF => Ok(DataType::Dictionary(
                        Box::new(key_field.data_type().clone()),
                        Box::new(value_field.data_type().clone()),
                    )),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    ),
                }
            }
            r#type::Kind::Decimal(d) => match d.type_variation_reference {
                DECIMAL_128_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal128(d.precision as u8, d.scale as i8))
                }
                DECIMAL_256_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal256(d.precision as u8, d.scale as i8))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::IntervalYear(_) => {
                Ok(DataType::Interval(IntervalUnit::YearMonth))
            }
            r#type::Kind::IntervalDay(i) => match i.type_variation_reference {
                DEFAULT_INTERVAL_DAY_TYPE_VARIATION_REF => {
                    Ok(DataType::Interval(IntervalUnit::DayTime))
                }
                DURATION_INTERVAL_DAY_TYPE_VARIATION_REF => {
                    let duration_unit = match i.precision {
                        Some(p) => from_substrait_precision(p, "Duration"),
                        None => {
                            not_impl_err!("Missing Substrait precision for Duration")
                        }
                    }?;
                    Ok(DataType::Duration(duration_unit))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::IntervalCompound(_) => {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            r#type::Kind::UserDefined(u) => {
                if let Ok(data_type) = consumer.consume_user_defined_type(u) {
                    return Ok(data_type);
                }

                // TODO: remove the code below once the producer has been updated
                if let Some(name) = consumer.get_extensions().types.get(&u.type_reference)
                {
                    #[expect(deprecated)]
                    match name.as_ref() {
                        // Kept for backwards compatibility, producers should use IntervalCompound instead
                        INTERVAL_MONTH_DAY_NANO_TYPE_NAME => {
                            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
                        }
                        FLOAT_16_TYPE_NAME => Ok(DataType::Float16),
                        NULL_TYPE_NAME => Ok(DataType::Null),
                        _ => not_impl_err!(
                            "Unsupported Substrait user defined type with ref {} and variation {}",
                            u.type_reference,
                            u.type_variation_reference
                        ),
                    }
                } else {
                    #[expect(deprecated)]
                    match u.type_reference {
                        // Kept for backwards compatibility, producers should use IntervalYear instead
                        INTERVAL_YEAR_MONTH_TYPE_REF => {
                            Ok(DataType::Interval(IntervalUnit::YearMonth))
                        }
                        // Kept for backwards compatibility, producers should use IntervalDay instead
                        INTERVAL_DAY_TIME_TYPE_REF => {
                            Ok(DataType::Interval(IntervalUnit::DayTime))
                        }
                        // Kept for backwards compatibility, producers should use IntervalCompound instead
                        INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
                        }
                        _ => not_impl_err!(
                            "Unsupported Substrait user defined type with ref {} and variation {}",
                            u.type_reference,
                            u.type_variation_reference
                        ),
                    }
                }
            }
            r#type::Kind::Struct(s) => Ok(DataType::Struct(from_substrait_struct_type(
                consumer, s, dfs_names, name_idx,
            )?)),
            r#type::Kind::Varchar(_) => Ok(DataType::Utf8),
            r#type::Kind::FixedChar(_) => Ok(DataType::Utf8),
            _ => not_impl_err!("Unsupported Substrait type: {s_kind:?}"),
        },
        _ => not_impl_err!("`None` Substrait kind is not supported"),
    }
}

/// Convert Substrait NamedStruct to DataFusion DFSchemaRef
pub fn from_substrait_named_struct(
    consumer: &impl SubstraitConsumer,
    base_schema: &NamedStruct,
) -> datafusion::common::Result<DFSchema> {
    let mut name_idx = 0;
    let fields = from_substrait_struct_type(
        consumer,
        base_schema.r#struct.as_ref().ok_or_else(|| {
            substrait_datafusion_err!("Named struct must contain a struct")
        })?,
        &base_schema.names,
        &mut name_idx,
    )?;
    if name_idx != base_schema.names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            base_schema.names.len()
        );
    }
    DFSchema::try_from(Schema::new(fields))
}

fn from_substrait_struct_type(
    consumer: &impl SubstraitConsumer,
    s: &r#type::Struct,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> datafusion::common::Result<Fields> {
    let mut fields = vec![];
    for (i, f) in s.types.iter().enumerate() {
        let name = next_struct_field_name(i, dfs_names, name_idx)?;
        let field = substrait_type_to_field(consumer, f, dfs_names, name_idx)?
            .with_name(name)
            .with_nullable(type_is_nullable(f)?);
        fields.push(field);
    }
    Ok(fields.into())
}

fn type_is_nullable(dt: &Type) -> datafusion::common::Result<bool> {
    let Some(kind) = dt.kind.as_ref() else {
        return Ok(true);
    };

    let nullability = match kind {
        r#type::Kind::Bool(boolean) => boolean.nullability,
        r#type::Kind::I8(integer) => integer.nullability,
        r#type::Kind::I16(integer) => integer.nullability,
        r#type::Kind::I32(integer) => integer.nullability,
        r#type::Kind::I64(integer) => integer.nullability,
        r#type::Kind::Fp32(float) => float.nullability,
        r#type::Kind::Fp64(float) => float.nullability,
        #[expect(deprecated)]
        r#type::Kind::Timestamp(timestamp) => timestamp.nullability,
        r#type::Kind::Date(date) => date.nullability,
        #[expect(deprecated)]
        r#type::Kind::Time(time) => time.nullability,
        #[expect(deprecated)]
        r#type::Kind::TimestampTz(timestamp) => timestamp.nullability,
        r#type::Kind::IntervalYear(interval) => interval.nullability,
        r#type::Kind::IntervalDay(interval) => interval.nullability,
        r#type::Kind::IntervalCompound(interval) => interval.nullability,
        r#type::Kind::Uuid(uuid) => uuid.nullability,
        r#type::Kind::String(string) => string.nullability,
        r#type::Kind::Binary(binary) => binary.nullability,
        r#type::Kind::FixedChar(fixed) => fixed.nullability,
        r#type::Kind::Varchar(varchar) => varchar.nullability,
        r#type::Kind::FixedBinary(fixed) => fixed.nullability,
        r#type::Kind::Decimal(decimal) => decimal.nullability,
        r#type::Kind::PrecisionTime(time) => time.nullability,
        r#type::Kind::PrecisionTimestamp(timestamp) => timestamp.nullability,
        r#type::Kind::PrecisionTimestampTz(timestamp) => timestamp.nullability,
        r#type::Kind::Struct(r#struct) => r#struct.nullability,
        r#type::Kind::List(list) => list.nullability,
        r#type::Kind::Map(map) => map.nullability,
        r#type::Kind::Func(func) => func.nullability,
        r#type::Kind::UserDefined(user_defined) => user_defined.nullability,
        #[expect(deprecated)]
        r#type::Kind::UserDefinedTypeReference(_) => r#type::Nullability::Required as i32,
        r#type::Kind::Alias(alias) => alias.nullability,
    };

    is_nullable(nullability)
}

fn is_nullable(nullability: i32) -> datafusion::common::Result<bool> {
    match r#type::Nullability::try_from(nullability) {
        Ok(r#type::Nullability::Required) => Ok(false),
        Ok(r#type::Nullability::Nullable | r#type::Nullability::Unspecified) => Ok(true),
        Err(_) => not_impl_err!("Unsupported Substrait Nullability value {nullability}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::DefaultSubstraitConsumer;
    use async_trait::async_trait;
    use datafusion::catalog::TableProvider;
    use datafusion::common::TableReference;
    use datafusion::execution::{FunctionRegistry, SessionState, SessionStateBuilder};
    use std::collections::HashMap;
    use std::sync::Arc;
    use substrait::proto::r#type::Kind;

    /// Wraps [DefaultSubstraitConsumer], overriding `consume_user_defined_type`
    /// (so a `UserDefined` type resolves instead of erroring) and
    /// `consume_type_metadata`, which returns the fixed metadata below for a
    /// user-defined type and nothing for every other type.
    struct MetadataConsumer<'a> {
        inner: DefaultSubstraitConsumer<'a>,
        metadata: Option<HashMap<String, String>>,
    }

    #[async_trait]
    impl SubstraitConsumer for MetadataConsumer<'_> {
        async fn resolve_table_ref(
            &self,
            table_ref: &TableReference,
        ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
            self.inner.resolve_table_ref(table_ref).await
        }

        fn get_extensions(&self) -> &Extensions {
            self.inner.get_extensions()
        }

        fn get_function_registry(&self) -> &impl FunctionRegistry {
            self.inner.get_function_registry()
        }

        fn consume_user_defined_type(
            &self,
            _user_defined_type: &r#type::UserDefined,
        ) -> datafusion::common::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn consume_type_metadata(
            &self,
            typ: &Type,
        ) -> datafusion::common::Result<Option<HashMap<String, String>>> {
            match typ.kind {
                Some(Kind::UserDefined(_)) => Ok(self.metadata.clone()),
                _ => Ok(None),
            }
        }
    }

    fn user_defined_type() -> Type {
        Type {
            kind: Some(Kind::UserDefined(r#type::UserDefined {
                type_reference: 0,
                type_variation_reference: 0,
                nullability: r#type::Nullability::Nullable as i32,
                type_parameters: vec![],
            })),
        }
    }

    fn user_defined_struct_type() -> r#type::Struct {
        r#type::Struct {
            types: vec![user_defined_type()],
            type_variation_reference: 0,
            nullability: r#type::Nullability::Nullable as i32,
        }
    }

    fn test_metadata() -> HashMap<String, String> {
        HashMap::from([("user-defined-type".to_string(), "json".to_string())])
    }

    fn consumer_with_metadata(
        metadata: Option<HashMap<String, String>>,
    ) -> MetadataConsumer<'static> {
        let extensions: &'static Extensions = Box::leak(Box::new(Extensions::default()));
        let session_state: &'static SessionState =
            Box::leak(Box::new(SessionStateBuilder::new().build()));
        MetadataConsumer {
            inner: DefaultSubstraitConsumer::new(extensions, session_state),
            metadata,
        }
    }

    #[test]
    fn struct_field_gets_consumer_supplied_metadata_for_user_defined_type() {
        let metadata = test_metadata();
        let consumer = consumer_with_metadata(Some(metadata.clone()));

        let fields = from_substrait_struct_type(
            &consumer,
            &user_defined_struct_type(),
            &["col".to_string()],
            &mut 0,
        )
        .unwrap();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "col");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[0].metadata(), &metadata);
    }

    #[test]
    fn top_level_field_gets_consumer_supplied_metadata() {
        let metadata = test_metadata();
        let consumer = consumer_with_metadata(Some(metadata.clone()));

        let field =
            field_from_substrait_type_without_names(&consumer, &user_defined_type())
                .unwrap();

        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(field.metadata(), &metadata);
    }

    #[test]
    fn named_top_level_field_gets_consumer_supplied_metadata() {
        // Covers `field_from_substrait_type`, the public entry point that
        // threads `dfs_names`/`name_idx` through, as opposed to the
        // `_without_names` variant used by the tests above.
        let metadata = test_metadata();
        let consumer = consumer_with_metadata(Some(metadata.clone()));

        let field = field_from_substrait_type(
            &consumer,
            &user_defined_type(),
            &["col".to_string()],
            &mut 0,
        )
        .unwrap();

        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(field.metadata(), &metadata);
    }

    #[test]
    fn list_element_field_gets_metadata_for_a_nested_user_defined_type() {
        let metadata = test_metadata();
        let consumer = consumer_with_metadata(Some(metadata.clone()));
        let list = Type {
            kind: Some(Kind::List(Box::new(r#type::List {
                r#type: Some(Box::new(user_defined_type())),
                type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                nullability: r#type::Nullability::Nullable as i32,
            }))),
        };

        let field = field_from_substrait_type_without_names(&consumer, &list).unwrap();

        let DataType::List(element) = field.data_type() else {
            panic!("expected a List, got {:?}", field.data_type());
        };
        assert_eq!(element.name(), Field::LIST_FIELD_DEFAULT_NAME);
        assert!(element.is_nullable());
        assert_eq!(element.metadata(), &metadata);
        // The list itself is not a user-defined type, so it carries no metadata.
        assert!(field.metadata().is_empty());
    }

    #[test]
    fn map_key_and_value_fields_get_metadata_for_a_nested_user_defined_type() {
        let metadata = test_metadata();
        let consumer = consumer_with_metadata(Some(metadata.clone()));
        let map = Type {
            kind: Some(Kind::Map(Box::new(r#type::Map {
                key: Some(Box::new(user_defined_type())),
                value: Some(Box::new(user_defined_type())),
                type_variation_reference: DEFAULT_MAP_TYPE_VARIATION_REF,
                nullability: r#type::Nullability::Nullable as i32,
            }))),
        };

        let field = field_from_substrait_type_without_names(&consumer, &map).unwrap();

        let DataType::Map(entries, _) = field.data_type() else {
            panic!("expected a Map, got {:?}", field.data_type());
        };
        let DataType::Struct(entry_fields) = entries.data_type() else {
            panic!("expected map entries to be a Struct");
        };
        assert_eq!(entry_fields[0].name(), "key");
        assert!(!entry_fields[0].is_nullable());
        assert_eq!(entry_fields[0].metadata(), &metadata);
        assert_eq!(entry_fields[1].name(), "value");
        assert!(entry_fields[1].is_nullable());
        assert_eq!(entry_fields[1].metadata(), &metadata);
    }

    #[test]
    fn struct_field_has_no_metadata_when_consumer_returns_none() {
        let consumer = consumer_with_metadata(None);

        let fields = from_substrait_struct_type(
            &consumer,
            &user_defined_struct_type(),
            &["col".to_string()],
            &mut 0,
        )
        .unwrap();

        assert_eq!(fields.len(), 1);
        assert!(fields[0].metadata().is_empty());
    }

    #[test]
    fn type_is_nullable_user_defined_type_reference_is_required() {
        // The deprecated `UserDefinedTypeReference` variant doesn't carry a
        // nullability field; the consumer hardcodes Required (non-null).
        #[expect(deprecated)]
        let dt = Type {
            kind: Some(Kind::UserDefinedTypeReference(0)),
        };
        assert!(!type_is_nullable(&dt).unwrap());
    }

    #[test]
    fn type_is_nullable_missing_kind_defaults_to_nullable() {
        // Defensive: a Type whose kind is None is treated as nullable.
        let dt = Type { kind: None };
        assert!(type_is_nullable(&dt).unwrap());
    }

    #[test]
    fn is_nullable_rejects_unrecognized_enum_value() {
        let err = is_nullable(i32::MAX).unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported Substrait Nullability"),
            "got: {err}"
        );
    }
}
