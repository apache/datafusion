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

use apache_avro::schema::{
    Alias, DecimalSchema, EnumSchema, FixedSchema, Name, RecordSchema,
};
use apache_avro::types::Value;
use apache_avro::Schema as AvroSchema;
use arrow::datatypes::{DataType, IntervalUnit, Schema, TimeUnit, UnionMode};
use arrow::datatypes::{Field, UnionFields};
use datafusion_common::error::Result;
use datafusion_common::HashSet;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
struct SchemaResolver {
    names_lookup: HashMap<Name, DataType>,
    in_progress: HashSet<Name>,
}

/// Converts an avro schema to an arrow schema
pub fn to_arrow_schema(avro_schema: &apache_avro::Schema) -> Result<Schema> {
    let mut schema_fields = vec![];
    match avro_schema {
        AvroSchema::Record(RecordSchema { fields, name, .. }) => {
            let mut resolver = SchemaResolver::default();
            resolver.in_progress.insert(name.clone());

            for field in fields {
                schema_fields.push(schema_to_field_with_props(
                    &field.schema,
                    Some(&field.name),
                    field.is_nullable(),
                    Some(external_props(&field.schema)),
                    &mut resolver,
                )?)
            }

            // Not really relevant anymore but for correctness
            resolver.in_progress.remove(name);
        }
        schema => schema_fields.push(schema_to_field(schema, Some(""), false)?),
    }

    let schema = Schema::new(schema_fields);
    Ok(schema)
}

fn schema_to_field(
    schema: &apache_avro::Schema,
    name: Option<&str>,
    nullable: bool,
) -> Result<Field> {
    schema_to_field_with_props(
        schema,
        name,
        nullable,
        Default::default(),
        &mut SchemaResolver::default(),
    )
}

fn schema_to_field_with_props(
    schema: &AvroSchema,
    name: Option<&str>,
    nullable: bool,
    props: Option<HashMap<String, String>>,
    resolver: &mut SchemaResolver,
) -> Result<Field> {
    let mut nullable = nullable;
    let field_type: DataType = match schema {
        AvroSchema::Ref { name } => {
            // We can't have an infinitely recursing schema in avro,
            // so return an error for these kinds of references
            if resolver.in_progress.contains(name) {
                return Err(apache_avro::Error::new(
                    apache_avro::error::Details::SchemaResolutionError(name.clone()),
                )
                .into());
            }

            if let Some(dt) = resolver.names_lookup.get(name) {
                dt.clone()
            } else {
                return Err(apache_avro::Error::new(
                    apache_avro::error::Details::SchemaResolutionError(name.clone()),
                )
                .into());
            }
        }
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int => DataType::Int32,
        AvroSchema::Long => DataType::Int64,
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes => DataType::Binary,
        AvroSchema::String => DataType::Utf8,
        AvroSchema::Array(item_schema) => {
            DataType::List(Arc::new(schema_to_field_with_props(
                &item_schema.items,
                Some("item"),
                false,
                None,
                resolver,
            )?))
        }
        AvroSchema::Map(value_schema) => {
            let value_field = schema_to_field_with_props(
                &value_schema.types,
                Some("value"),
                false,
                None,
                resolver,
            )?;
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(value_field.data_type().clone()),
            )
        }
        AvroSchema::Union(us) => {
            // If there are only two variants and one of them is null, set the other type as the field data type
            let has_nullable = us
                .find_schema_with_known_schemata::<apache_avro::Schema>(
                    &Value::Null,
                    None,
                    &None,
                )
                .is_some();
            let sub_schemas = us.variants();
            if has_nullable && sub_schemas.len() == 2 {
                nullable = true;
                if let Some(schema) = sub_schemas
                    .iter()
                    .find(|&schema| !matches!(schema, AvroSchema::Null))
                {
                    schema_to_field_with_props(
                        schema,
                        None,
                        has_nullable,
                        None,
                        resolver,
                    )?
                    .data_type()
                    .clone()
                } else {
                    return Err(apache_avro::Error::new(
                        apache_avro::error::Details::GetUnionDuplicate,
                    )
                    .into());
                }
            } else {
                let fields = sub_schemas
                    .iter()
                    .map(|s| {
                        schema_to_field_with_props(s, None, has_nullable, None, resolver)
                    })
                    .collect::<Result<Vec<Field>>>()?;
                let type_ids = 0_i8..fields.len() as i8;
                DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Dense)
            }
        }
        AvroSchema::Record(RecordSchema { fields, name, .. }) => {
            let inserted = resolver.in_progress.insert(name.clone());
            if !inserted {
                return Err(apache_avro::Error::new(
                    apache_avro::error::Details::SchemaResolutionError(name.clone()),
                )
                .into());
            }

            let fields: Result<_> = fields
                .iter()
                .map(|field| {
                    let mut props = HashMap::new();
                    if let Some(doc) = &field.doc {
                        props.insert("avro::doc".to_string(), doc.clone());
                    }
                    /*if let Some(aliases) = fields.aliases {
                        props.insert("aliases", aliases);
                    }*/
                    schema_to_field_with_props(
                        &field.schema,
                        Some(&field.name),
                        false,
                        Some(props),
                        resolver,
                    )
                })
                .collect();

            let dtype = DataType::Struct(fields?);

            let previous = resolver.names_lookup.insert(name.clone(), dtype.clone());
            if previous.is_some() {
                return Err(apache_avro::Error::new(
                    apache_avro::error::Details::SchemaResolutionError(name.clone()),
                )
                .into());
            }
            resolver.in_progress.remove(name);

            dtype
        }
        AvroSchema::Enum(EnumSchema { name, .. }) => {
            let dtype = DataType::Utf8;

            let existing = resolver.names_lookup.insert(name.clone(), dtype.clone());
            if existing.is_some() {
                return Err(apache_avro::Error::new(
                    apache_avro::error::Details::SchemaResolutionError(name.clone()),
                )
                .into());
            }

            dtype
        }
        AvroSchema::Fixed(FixedSchema { size, name, .. }) => {
            let dtype = DataType::FixedSizeBinary(*size as i32);

            let existing = resolver.names_lookup.insert(name.clone(), dtype.clone());
            if existing.is_some() {
                return Err(apache_avro::Error::new(
                    apache_avro::error::Details::SchemaResolutionError(name.clone()),
                )
                .into());
            }

            dtype
        }
        AvroSchema::Decimal(DecimalSchema {
            precision, scale, ..
        }) => DataType::Decimal128(*precision as u8, *scale as i8),
        AvroSchema::BigDecimal => DataType::LargeBinary,
        AvroSchema::Uuid => DataType::FixedSizeBinary(16),
        AvroSchema::Date => DataType::Date32,
        AvroSchema::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
        AvroSchema::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        AvroSchema::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        AvroSchema::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        AvroSchema::TimestampNanos => DataType::Timestamp(TimeUnit::Nanosecond, None),
        AvroSchema::LocalTimestampMillis => todo!(),
        AvroSchema::LocalTimestampMicros => todo!(),
        AvroSchema::LocalTimestampNanos => todo!(),
        AvroSchema::Duration => DataType::Duration(TimeUnit::Millisecond),
    };

    let data_type = field_type.clone();
    let name = name.unwrap_or_else(|| default_field_name(&data_type));

    let mut field = Field::new(name, field_type, nullable);
    field.set_metadata(props.unwrap_or_default());
    Ok(field)
}

fn default_field_name(dt: &DataType) -> &str {
    match dt {
        DataType::Null => "null",
        DataType::Boolean => "bit",
        DataType::Int8 => "tinyint",
        DataType::Int16 => "smallint",
        DataType::Int32 => "int",
        DataType::Int64 => "bigint",
        DataType::UInt8 => "uint1",
        DataType::UInt16 => "uint2",
        DataType::UInt32 => "uint4",
        DataType::UInt64 => "uint8",
        DataType::Float16 => "float2",
        DataType::Float32 => "float4",
        DataType::Float64 => "float8",
        DataType::Date32 => "dateday",
        DataType::Date64 => "datemilli",
        DataType::Time32(tu) | DataType::Time64(tu) => match tu {
            TimeUnit::Second => "timesec",
            TimeUnit::Millisecond => "timemilli",
            TimeUnit::Microsecond => "timemicro",
            TimeUnit::Nanosecond => "timenano",
        },
        DataType::Timestamp(tu, tz) => {
            if tz.is_some() {
                match tu {
                    TimeUnit::Second => "timestampsectz",
                    TimeUnit::Millisecond => "timestampmillitz",
                    TimeUnit::Microsecond => "timestampmicrotz",
                    TimeUnit::Nanosecond => "timestampnanotz",
                }
            } else {
                match tu {
                    TimeUnit::Second => "timestampsec",
                    TimeUnit::Millisecond => "timestampmilli",
                    TimeUnit::Microsecond => "timestampmicro",
                    TimeUnit::Nanosecond => "timestampnano",
                }
            }
        }
        DataType::Duration(_) => "duration",
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => "intervalyear",
            IntervalUnit::DayTime => "intervalmonth",
            IntervalUnit::MonthDayNano => "intervalmonthdaynano",
        },
        DataType::Binary => "varbinary",
        DataType::FixedSizeBinary(_) => "fixedsizebinary",
        DataType::LargeBinary => "largevarbinary",
        DataType::Utf8 => "varchar",
        DataType::LargeUtf8 => "largevarchar",
        DataType::List(_) => "list",
        DataType::FixedSizeList(_, _) => "fixed_size_list",
        DataType::LargeList(_) => "largelist",
        DataType::Struct(_) => "struct",
        DataType::Union(_, _) => "union",
        DataType::Dictionary(_, _) => "map",
        DataType::Map(_, _) => unimplemented!("Map support not implemented"),
        DataType::RunEndEncoded(_, _) => {
            unimplemented!("RunEndEncoded support not implemented")
        }
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::ListView(_)
        | DataType::LargeListView(_) => {
            unimplemented!("View support not implemented")
        }
        DataType::Decimal32(_, _) => "decimal",
        DataType::Decimal64(_, _) => "decimal",
        DataType::Decimal128(_, _) => "decimal",
        DataType::Decimal256(_, _) => "decimal",
    }
}

fn external_props(schema: &AvroSchema) -> HashMap<String, String> {
    let mut props = HashMap::new();
    match &schema {
        AvroSchema::Record(RecordSchema {
            doc: Some(ref doc), ..
        })
        | AvroSchema::Enum(EnumSchema {
            doc: Some(ref doc), ..
        })
        | AvroSchema::Fixed(FixedSchema {
            doc: Some(ref doc), ..
        }) => {
            props.insert("avro::doc".to_string(), doc.clone());
        }
        _ => {}
    }
    match &schema {
        AvroSchema::Record(RecordSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        })
        | AvroSchema::Enum(EnumSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        })
        | AvroSchema::Fixed(FixedSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        }) => {
            let aliases: Vec<String> = aliases
                .iter()
                .map(|alias| aliased(alias, namespace.as_deref(), None))
                .collect();
            props.insert(
                "avro::aliases".to_string(),
                format!("[{}]", aliases.join(",")),
            );
        }
        _ => {}
    }
    props
}

/// Returns the fully qualified name for a field
pub fn aliased(
    alias: &Alias,
    namespace: Option<&str>,
    default_namespace: Option<&str>,
) -> String {
    if alias.namespace().is_some() {
        alias.fullname(None)
    } else {
        let namespace = namespace.as_ref().copied().or(default_namespace);

        match namespace {
            Some(ref namespace) => format!("{}.{}", namespace, alias.name()),
            None => alias.fullname(None),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{aliased, external_props, to_arrow_schema};
    use apache_avro::schema::{Alias, EnumSchema, FixedSchema, Name, RecordSchema};
    use apache_avro::Schema as AvroSchema;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::DataFusionError;

    fn alias(name: &str) -> Alias {
        Alias::new(name).unwrap()
    }

    #[test]
    fn test_alias() {
        assert_eq!(aliased(&alias("foo.bar"), None, None), "foo.bar");
        assert_eq!(aliased(&alias("bar"), Some("foo"), None), "foo.bar");
        assert_eq!(aliased(&alias("bar"), Some("foo"), Some("cat")), "foo.bar");
        assert_eq!(aliased(&alias("bar"), None, Some("cat")), "cat.bar");
    }

    #[test]
    fn test_external_props() {
        let record_schema = AvroSchema::Record(RecordSchema {
            name: Name {
                name: "record".to_string(),
                namespace: None,
            },
            aliases: Some(vec![alias("fooalias"), alias("baralias")]),
            doc: Some("record documentation".to_string()),
            fields: vec![],
            lookup: Default::default(),
            attributes: Default::default(),
        });
        let props = external_props(&record_schema);
        assert_eq!(
            props.get("avro::doc"),
            Some(&"record documentation".to_string())
        );
        assert_eq!(
            props.get("avro::aliases"),
            Some(&"[fooalias,baralias]".to_string())
        );
        let enum_schema = AvroSchema::Enum(EnumSchema {
            name: Name {
                name: "enum".to_string(),
                namespace: None,
            },
            aliases: Some(vec![alias("fooenum"), alias("barenum")]),
            doc: Some("enum documentation".to_string()),
            symbols: vec![],
            default: None,
            attributes: Default::default(),
        });
        let props = external_props(&enum_schema);
        assert_eq!(
            props.get("avro::doc"),
            Some(&"enum documentation".to_string())
        );
        assert_eq!(
            props.get("avro::aliases"),
            Some(&"[fooenum,barenum]".to_string())
        );
        let fixed_schema = AvroSchema::Fixed(FixedSchema {
            name: Name {
                name: "fixed".to_string(),
                namespace: None,
            },
            aliases: Some(vec![alias("foofixed"), alias("barfixed")]),
            size: 1,
            doc: None,
            default: None,
            attributes: Default::default(),
        });
        let props = external_props(&fixed_schema);
        assert_eq!(
            props.get("avro::aliases"),
            Some(&"[foofixed,barfixed]".to_string())
        );
    }

    #[test]
    fn test_invalid_avro_schema() {}

    #[test]
    fn test_plain_types_schema() {
        let schema = AvroSchema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "topLevelRecord",
              "fields" : [ {
                "name" : "id",
                "type" : [ "int", "null" ]
              }, {
                "name" : "bool_col",
                "type" : [ "boolean", "null" ]
              }, {
                "name" : "tinyint_col",
                "type" : [ "int", "null" ]
              }, {
                "name" : "smallint_col",
                "type" : [ "int", "null" ]
              }, {
                "name" : "int_col",
                "type" : [ "int", "null" ]
              }, {
                "name" : "bigint_col",
                "type" : [ "long", "null" ]
              }, {
                "name" : "float_col",
                "type" : [ "float", "null" ]
              }, {
                "name" : "double_col",
                "type" : [ "double", "null" ]
              }, {
                "name" : "date_string_col",
                "type" : [ "bytes", "null" ]
              }, {
                "name" : "string_col",
                "type" : [ "bytes", "null" ]
              }, {
                "name" : "timestamp_col",
                "type" : [ {
                  "type" : "long",
                  "logicalType" : "timestamp-micros"
                }, "null" ]
              } ]
            }"#,
        );
        assert!(schema.is_ok(), "{schema:?}");
        let arrow_schema = to_arrow_schema(&schema.unwrap());
        assert!(arrow_schema.is_ok(), "{arrow_schema:?}");
        let expected = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("tinyint_col", DataType::Int32, true),
            Field::new("smallint_col", DataType::Int32, true),
            Field::new("int_col", DataType::Int32, true),
            Field::new("bigint_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float32, true),
            Field::new("double_col", DataType::Float64, true),
            Field::new("date_string_col", DataType::Binary, true),
            Field::new("string_col", DataType::Binary, true),
            Field::new(
                "timestamp_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);
        assert_eq!(arrow_schema.unwrap(), expected);
    }

    #[test]
    fn test_nested_schema() {
        let avro_schema = apache_avro::Schema::parse_str(
            r#"
            {
              "type": "record",
              "name": "r1",
              "fields": [
                {
                  "name": "col1",
                  "type": [
                    "null",
                    {
                      "type": "record",
                      "name": "r2",
                      "fields": [
                        {
                          "name": "col2",
                          "type": "string"
                        },
                        {
                          "name": "col3",
                          "type": ["null", "string"],
                          "default": null
                        }
                      ]
                    }
                  ],
                  "default": null
                }
              ]
            }"#,
        )
        .unwrap();
        // should not use Avro Record names.
        let expected_arrow_schema = Schema::new(vec![Field::new(
            "col1",
            DataType::Struct(
                vec![
                    Field::new("col2", DataType::Utf8, false),
                    Field::new("col3", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        )]);
        assert_eq!(
            to_arrow_schema(&avro_schema).unwrap(),
            expected_arrow_schema
        );
    }

    #[test]
    fn test_non_record_schema() {
        let arrow_schema = to_arrow_schema(&AvroSchema::String);
        assert!(arrow_schema.is_ok(), "{arrow_schema:?}");
        assert_eq!(
            arrow_schema.unwrap(),
            Schema::new(vec![Field::new("", DataType::Utf8, false)])
        );
    }

    #[test]
    fn test_self_referential_record_rejected() {
        // Self-referential schemas should be rejected
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Person",
              "fields": [
                {
                  "name": "name",
                  "type": "string"
                },
                {
                  "name": "friend",
                  "type": ["null", "Person"]
                }
              ]
            }"#,
        )
        .unwrap();

        let result = to_arrow_schema(&avro_schema);
        let DataFusionError::AvroError(err) = result.as_ref().unwrap_err() else {
            panic!("Expected AvroError but got {result:?}");
        };

        let apache_avro::error::Details::SchemaResolutionError(name) = err.details()
        else {
            panic!("Expected SchemaResolutionError but got {:?}", err.details());
        };

        assert_eq!(name.name, "Person");
        assert_eq!(name.namespace, None);
    }

    #[test]
    fn test_mutually_recursive_records_rejected() {
        // Mutually recursive schemas should be rejected
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Node",
              "fields": [
                {
                  "name": "value",
                  "type": "int"
                },
                {
                  "name": "children",
                  "type": {
                    "type": "array",
                    "items": "Node"
                  }
                }
              ]
            }"#,
        )
        .unwrap();

        let result = to_arrow_schema(&avro_schema);
        assert!(
            result.is_err(),
            "Self-referential array schemas should be rejected"
        );
    }

    #[test]
    fn test_enum_ref() {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Message",
              "fields": [
                {
                  "name": "priority",
                  "type": {
                    "type": "enum",
                    "name": "Priority",
                    "symbols": ["LOW", "MEDIUM", "HIGH"]
                  }
                },
                {
                  "name": "fallback_priority",
                  "type": ["null", "Priority"]
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(1).is_nullable());
    }

    #[test]
    fn test_fixed_ref() {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "HashRecord",
              "fields": [
                {
                  "name": "primary_hash",
                  "type": {
                    "type": "fixed",
                    "name": "MD5",
                    "size": 16
                  }
                },
                {
                  "name": "secondary_hash",
                  "type": ["null", "MD5"]
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            arrow_schema.field(1).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(arrow_schema.field(1).is_nullable());
    }

    #[test]
    fn test_multiple_refs_same_type() {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Container",
              "fields": [
                {
                  "name": "status1",
                  "type": {
                    "type": "enum",
                    "name": "Status",
                    "symbols": ["ACTIVE", "INACTIVE"]
                  }
                },
                {
                  "name": "status2",
                  "type": "Status"
                },
                {
                  "name": "status3",
                  "type": ["null", "Status"]
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 3);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Utf8);
        assert!(arrow_schema.field(2).is_nullable());
    }

    #[test]
    fn test_non_recursive_nested_record_ref() {
        // Non-recursive nested records with refs should work
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Outer",
              "fields": [
                {
                  "name": "id_type",
                  "type": {
                    "type": "fixed",
                    "name": "UUID",
                    "size": 16
                  }
                },
                {
                  "name": "nested",
                  "type": {
                    "type": "record",
                    "name": "Inner",
                    "fields": [
                      {
                        "name": "inner_id",
                        "type": "UUID"
                      }
                    ]
                  }
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::FixedSizeBinary(16)
        );

        if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].data_type(), &DataType::FixedSizeBinary(16));
        } else {
            panic!("Expected Struct type for nested field");
        }
    }

    #[test]
    fn test_ref_in_array() {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "ArrayContainer",
              "fields": [
                {
                  "name": "priority_def",
                  "type": {
                    "type": "enum",
                    "name": "Priority",
                    "symbols": ["LOW", "HIGH"]
                  }
                },
                {
                  "name": "priorities",
                  "type": {
                    "type": "array",
                    "items": "Priority"
                  }
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);

        if let DataType::List(item_field) = arrow_schema.field(1).data_type() {
            assert_eq!(item_field.data_type(), &DataType::Utf8);
        } else {
            panic!("Expected List type for array field");
        }
    }

    #[test]
    fn test_invalid_ref() {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "BadRecord",
              "fields": [
                {
                  "name": "bad_ref",
                  "type": "NonExistentType"
                }
              ]
            }"#,
        );

        // This should either fail during Avro parsing or during Arrow conversion
        assert!(avro_schema.is_err() || to_arrow_schema(&avro_schema.unwrap()).is_err());
    }

    #[test]
    fn test_namespaced_ref() {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Container",
              "namespace": "com.example",
              "fields": [
                {
                  "name": "status_def",
                  "type": {
                    "type": "enum",
                    "name": "Status",
                    "namespace": "com.example.types",
                    "symbols": ["OK", "ERROR"]
                  }
                },
                {
                  "name": "status_ref",
                  "type": "com.example.types.Status"
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_complex_non_recursive_ref_graph() {
        // Multiple types referencing each other without cycles
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Root",
              "fields": [
                {
                  "name": "hash_type",
                  "type": {
                    "type": "fixed",
                    "name": "Hash",
                    "size": 32
                  }
                },
                {
                  "name": "status_type",
                  "type": {
                    "type": "enum",
                    "name": "Status",
                    "symbols": ["PENDING", "COMPLETE"]
                  }
                },
                {
                  "name": "data",
                  "type": {
                    "type": "record",
                    "name": "Data",
                    "fields": [
                      {
                        "name": "id",
                        "type": "Hash"
                      },
                      {
                        "name": "state",
                        "type": "Status"
                      }
                    ]
                  }
                },
                {
                  "name": "backup_hash",
                  "type": ["null", "Hash"]
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert_eq!(arrow_schema.field(1).data_type(), &DataType::Utf8);

        if let DataType::Struct(fields) = arrow_schema.field(2).data_type() {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].data_type(), &DataType::FixedSizeBinary(32));
            assert_eq!(fields[1].data_type(), &DataType::Utf8);
        } else {
            panic!("Expected Struct type for data field");
        }

        assert_eq!(
            arrow_schema.field(3).data_type(),
            &DataType::FixedSizeBinary(32)
        );
        assert!(arrow_schema.field(3).is_nullable());
    }

    #[test]
    fn test_duplicate_type_name_rejected() {
        // Defining the same named type twice should be rejected
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Container",
              "fields": [
                {
                  "name": "first",
                  "type": {
                    "type": "enum",
                    "name": "Status",
                    "symbols": ["OK"]
                  }
                },
                {
                  "name": "second",
                  "type": {
                    "type": "enum",
                    "name": "Status",
                    "symbols": ["ERROR"]
                  }
                }
              ]
            }"#,
        );

        // Avro parser itself should reject this, or our converter should
        assert!(avro_schema.is_err() || to_arrow_schema(&avro_schema.unwrap()).is_err());
    }

    #[test]
    fn test_deeply_nested_ref_chain() {
        // Test a chain of references without cycles
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
              "type": "record",
              "name": "Level1",
              "fields": [
                {
                  "name": "id_type",
                  "type": {
                    "type": "fixed",
                    "name": "ID",
                    "size": 8
                  }
                },
                {
                  "name": "level2",
                  "type": {
                    "type": "record",
                    "name": "Level2",
                    "fields": [
                      {
                        "name": "id",
                        "type": "ID"
                      },
                      {
                        "name": "level3",
                        "type": {
                          "type": "record",
                          "name": "Level3",
                          "fields": [
                            {
                              "name": "id",
                              "type": "ID"
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }"#,
        )
        .unwrap();

        let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(
            arrow_schema.field(0).data_type(),
            &DataType::FixedSizeBinary(8)
        );

        // Verify the nested structure
        if let DataType::Struct(level2_fields) = arrow_schema.field(1).data_type() {
            assert_eq!(level2_fields.len(), 2);
            assert_eq!(level2_fields[0].data_type(), &DataType::FixedSizeBinary(8));

            if let DataType::Struct(level3_fields) = level2_fields[1].data_type() {
                assert_eq!(level3_fields.len(), 1);
                assert_eq!(level3_fields[0].data_type(), &DataType::FixedSizeBinary(8));
            } else {
                panic!("Expected Struct type for level3");
            }
        } else {
            panic!("Expected Struct type for level2");
        }
    }
}
