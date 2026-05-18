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

//! Spark-compatible Parquet schema adapter.
//!
//! [`SparkPhysicalExprAdapterFactory`] implements
//! [`PhysicalExprAdapterFactory`] so it can be plugged into a
//! [`FileScanConfig`] via `with_expr_adapter`. The resulting adapter rewrites
//! physical expressions at planning time so that column references against
//! the logical (query) schema resolve correctly to the physical (file)
//! schema, while preserving Spark's vectorized-reader semantics:
//!
//! * case-insensitive name matching with duplicate detection (Spark's
//!   `_LEGACY_ERROR_TEMP_2093`),
//! * Parquet field-id matching with duplicate detection
//!   (`_LEGACY_ERROR_TEMP_2094`) and the missing-field-id runtime error,
//! * type-promotion rejection rules that mirror
//!   `ParquetVectorUpdaterFactory.getUpdater` in Spark, deferred to runtime
//!   when necessary so empty Parquet files (SPARK-26709) still pass,
//! * default values for columns that are missing from the file schema, and
//! * Spark-compatible casts via [`SparkCastColumnExpr`] for nested types.
//!
//! [`FileScanConfig`]: https://docs.rs/datafusion-datasource/latest/datafusion_datasource/file_scan_config/struct.FileScanConfig.html

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_physical_expr::expressions::{CastExpr, Column};
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
    replace_columns_with_literals,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::cast_column::SparkCastColumnExpr;
use super::error::ParquetSchemaError;
use super::options::SparkParquetOptions;

/// Factory that creates Spark-compatible [`PhysicalExprAdapter`] instances.
///
/// Plug this into a `FileScanConfig` via `with_expr_adapter` to read Parquet
/// files with Spark's vectorized-reader semantics.
#[derive(Clone, Debug)]
pub struct SparkPhysicalExprAdapterFactory {
    /// Spark-specific Parquet options for type conversions.
    parquet_options: SparkParquetOptions,
    /// Default values for columns that may be missing from the physical
    /// schema. The key is a `Column` (containing name and index).
    default_values: Option<HashMap<Column, ScalarValue>>,
}

impl SparkPhysicalExprAdapterFactory {
    /// Create a new factory with the given options and optional default
    /// values for missing columns.
    pub fn new(
        parquet_options: SparkParquetOptions,
        default_values: Option<HashMap<Column, ScalarValue>>,
    ) -> Self {
        Self {
            parquet_options,
            default_values,
        }
    }
}

/// Read the Parquet field id stored under arrow-rs's
/// `PARQUET_FIELD_ID_META_KEY`.
fn parse_field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse::<i32>().ok())
}

fn schema_has_field_ids(schema: &SchemaRef) -> bool {
    schema.fields().iter().any(|f| parse_field_id(f).is_some())
}

/// Remap physical schema field names to match logical schema field names.
/// Mirrors Spark's `clipParquetGroupFields`: prefer ID match for any logical
/// field that carries a `PARQUET:field_id`; fall back to case-insensitive
/// name match otherwise.
///
/// The remap only changes top-level field NAMES so that
/// [`DefaultPhysicalExprAdapter`]'s exact-name lookup hits. Indices, types,
/// nullability, and metadata stay as in the file. Returns the rewritten
/// schema and a `logical_name -> original_physical_name` map used downstream
/// to restore the original physical names before stream consumption.
///
/// [`DefaultPhysicalExprAdapter`]: datafusion_physical_expr_adapter::DefaultPhysicalExprAdapter
fn remap_physical_schema(
    logical_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    case_sensitive: bool,
    use_field_id: bool,
    ignore_missing_field_id: bool,
) -> Result<(SchemaRef, HashMap<String, String>)> {
    let should_match_by_id = use_field_id && schema_has_field_ids(logical_schema);

    if should_match_by_id
        && !ignore_missing_field_id
        && !schema_has_field_ids(physical_schema)
    {
        // Mirrors `ParquetReadSupport.inferSchema`'s eager check (Spark throws
        // a runtime error rather than silently returning null columns).
        return Err(ParquetSchemaError::MissingFieldIds.into());
    }

    // Build id -> all matching physical field names. We need the full list so
    // we can mirror Spark's `_LEGACY_ERROR_TEMP_2094` "Found duplicate field(s)"
    // error when an ID-bearing logical field would resolve to more than one
    // physical field.
    let mut id_to_phys_names: HashMap<i32, Vec<String>> = HashMap::new();
    if should_match_by_id {
        for pf in physical_schema.fields() {
            if let Some(id) = parse_field_id(pf) {
                id_to_phys_names
                    .entry(id)
                    .or_default()
                    .push(pf.name().clone());
            }
        }
        for lf in logical_schema.fields() {
            if let Some(id) = parse_field_id(lf)
                && let Some(matches) = id_to_phys_names.get(&id)
                && matches.len() > 1
            {
                return Err(ParquetSchemaError::DuplicateFieldByFieldId {
                    required_id: id,
                    matched_fields: matches.join(", "),
                }
                .into());
            }
        }
    }

    // Pre-build id -> first matching logical field for the per-physical
    // rename pass below.
    let id_to_logical: HashMap<i32, &FieldRef> = if should_match_by_id {
        let mut map = HashMap::new();
        for lf in logical_schema.fields() {
            if let Some(id) = parse_field_id(lf) {
                map.entry(id).or_insert(lf);
            }
        }
        map
    } else {
        HashMap::new()
    };

    // Names of ID-bearing logical fields whose ID is not present in the file.
    // Any physical field that shares one of these names must be renamed to
    // something the `DefaultPhysicalExprAdapter` cannot name-match, otherwise
    // the read would silently fall through to a name match. Spark's
    // `matchIdField` solves the same problem with `generateFakeColumnName`
    // (see `ParquetReadSupport.scala`).
    let unmatched_id_logical_names: HashSet<String> = if should_match_by_id {
        logical_schema
            .fields()
            .iter()
            .filter_map(|lf| {
                parse_field_id(lf).and_then(|id| {
                    if id_to_phys_names.contains_key(&id) {
                        None
                    } else {
                        Some(lf.name().clone())
                    }
                })
            })
            .collect()
    } else {
        HashSet::new()
    };
    let mut fake_counter: usize = 0;

    let mut name_map: HashMap<String, String> = HashMap::new();
    let remapped_fields: Vec<FieldRef> = physical_schema
        .fields()
        .iter()
        .map(|field| {
            // ID match first when the logical schema is ID-bearing.
            if should_match_by_id
                && let Some(phys_id) = parse_field_id(field)
                && let Some(logical_field) = id_to_logical.get(&phys_id)
            {
                if logical_field.name() != field.name() {
                    name_map.insert(logical_field.name().clone(), field.name().clone());
                    return Arc::new(
                        Field::new(
                            logical_field.name(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                        .with_metadata(field.metadata().clone()),
                    );
                }
                return Arc::clone(field);
            }

            // Block accidental name match for ID-bearing logical fields whose
            // ID is missing from the file. Mirrors Spark's
            // `generateFakeColumnName` in `matchIdField`.
            if should_match_by_id
                && unmatched_id_logical_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(field.name()))
            {
                fake_counter += 1;
                let fake_name = format!("__datafusion_unmatched_field_id_{fake_counter}");
                return Arc::new(
                    Field::new(fake_name, field.data_type().clone(), field.is_nullable())
                        .with_metadata(field.metadata().clone()),
                );
            }

            // Name match. Spark's `matchIdField` does not fall through to a
            // name match for ID-bearing logical fields, so skip those when
            // the schema is ID-bearing.
            if !case_sensitive {
                let logical_field = logical_schema.fields().iter().find(|lf| {
                    let lf_has_id = should_match_by_id && parse_field_id(lf).is_some();
                    !lf_has_id && lf.name().eq_ignore_ascii_case(field.name())
                });
                if let Some(logical_field) = logical_field
                    && logical_field.name() != field.name()
                {
                    name_map.insert(logical_field.name().clone(), field.name().clone());
                    return Arc::new(
                        Field::new(
                            logical_field.name(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        )
                        .with_metadata(field.metadata().clone()),
                    );
                }
            }

            Arc::clone(field)
        })
        .collect();

    Ok((Arc::new(Schema::new(remapped_fields)), name_map))
}

/// Format an Arrow `DataType` as Spark's catalog string (e.g. `Int64` ->
/// `bigint`), so `SchemaColumnConvertNotSupportedException` messages match
/// Spark's vectorized reader.
fn spark_catalog_name(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".to_string(),
        DataType::Binary | DataType::LargeBinary => "binary".to_string(),
        DataType::Date32 => "date".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        DataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            format!("decimal({p},{s})")
        }
        _ => "unknown".to_string(),
    }
}

/// Format an Arrow `DataType` as the Parquet primitive type name (e.g.
/// `Int64` -> `INT64`, matching `PrimitiveTypeName.toString()` in parquet-mr).
fn parquet_primitive_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "INT32",
        DataType::Int64 => "INT64",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => "BINARY",
        // Spark stores DATE as INT32 with a DATE logical-type annotation.
        DataType::Date32 => "INT32",
        // Spark stores TIMESTAMP as INT64 with a timestamp annotation, or as
        // INT96 (legacy nanos). arrow-rs surfaces both as `Timestamp`;
        // without the original physical name we report INT64, which matches
        // the common case.
        DataType::Timestamp(_, _) => "INT64",
        // Mirror Spark's `SparkToParquetSchemaConverter` decimal mapping:
        // precision 1-9 -> INT32, 10-18 -> INT64, 19+ -> FIXED_LEN_BYTE_ARRAY.
        DataType::Decimal128(p, _) | DataType::Decimal256(p, _) => {
            if *p <= 9 {
                "INT32"
            } else if *p <= 18 {
                "INT64"
            } else {
                "FIXED_LEN_BYTE_ARRAY"
            }
        }
        _ => "UNKNOWN",
    }
}

fn is_string_or_binary(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    )
}

/// Build a Spark-shaped `SchemaColumnConvertNotSupportedException` carrier
/// for a rejected Parquet -> Spark conversion. The bracketed column wrapping
/// mirrors `Arrays.toString(descriptor.getPath())` in Spark's vectorized
/// reader.
fn parquet_schema_convert_err(
    field_name: &str,
    physical_type: &DataType,
    target_type: &DataType,
) -> DataFusionError {
    ParquetSchemaError::SchemaConvert {
        file_path: String::new(),
        column: format!("[{field_name}]"),
        physical_type: parquet_primitive_name(physical_type).to_string(),
        spark_type: spark_catalog_name(target_type),
    }
    .into()
}

/// Build a [`RejectOnNonEmpty`] expr wrapping `child`. The rejection fires
/// only when the input batch is non-empty (mirrors Spark's per-row-group
/// check).
fn reject_on_non_empty_expr(
    child: Arc<dyn PhysicalExpr>,
    target_field: &FieldRef,
    field_name: &str,
    physical_type: &DataType,
    target_type: &DataType,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(RejectOnNonEmpty {
        child,
        target_field: Arc::clone(target_field),
        column: format!("[{field_name}]"),
        physical_type: parquet_primitive_name(physical_type).to_string(),
        spark_type: spark_catalog_name(target_type),
    })
}

/// Check if a specific column name has duplicate matches in the physical
/// schema (case-insensitive). Returns the error info if so.
fn check_column_duplicate(
    col_name: &str,
    physical_schema: &SchemaRef,
) -> Option<(String, String)> {
    let matches: Vec<&str> = physical_schema
        .fields()
        .iter()
        .filter(|pf| pf.name().eq_ignore_ascii_case(col_name))
        .map(|pf| pf.name().as_str())
        .collect();
    if matches.len() > 1 {
        // Include brackets to match the format expected by Spark's shim.
        Some((col_name.to_string(), format!("[{}]", matches.join(", "))))
    } else {
        None
    }
}

impl PhysicalExprAdapterFactory for SparkPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        // Remap physical schema field names to match logical names by Parquet
        // field id (when the logical schema carries IDs and `use_field_id` is
        // set) and/or by case-insensitive name match. The
        // `DefaultPhysicalExprAdapter` uses exact name matching, so without
        // this remapping, columns whose file names differ from the logical
        // names won't match and will be filled with NULLs.
        //
        // We also keep a reverse map (logical name -> original physical name)
        // so that after the default adapter produces expressions, we can
        // remap column names back to the original physical names. This is
        // necessary because downstream code looks up columns by name in the
        // actual stream schema, which uses the original physical file column
        // names.
        let needs_remap = !self.parquet_options.case_sensitive
            || (self.parquet_options.use_field_id
                && schema_has_field_ids(&logical_file_schema));
        let (
            adapted_physical_schema,
            logical_to_physical_names,
            original_physical_schema,
        ) = if needs_remap {
            let (remapped, logical_to_physical) = remap_physical_schema(
                &logical_file_schema,
                &physical_file_schema,
                self.parquet_options.case_sensitive,
                self.parquet_options.use_field_id,
                self.parquet_options.ignore_missing_field_id,
            )?;
            (
                remapped,
                if logical_to_physical.is_empty() {
                    None
                } else {
                    Some(logical_to_physical)
                },
                // Keep the original physical schema for per-column
                // duplicate detection. Only meaningful in
                // case-insensitive mode (matches existing behavior).
                if !self.parquet_options.case_sensitive {
                    Some(Arc::clone(&physical_file_schema))
                } else {
                    None
                },
            )
        } else {
            (Arc::clone(&physical_file_schema), None, None)
        };

        let default_factory = DefaultPhysicalExprAdapterFactory;
        let default_adapter = default_factory.create(
            Arc::clone(&logical_file_schema),
            Arc::clone(&adapted_physical_schema),
        )?;

        Ok(Arc::new(SparkPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema: adapted_physical_schema,
            parquet_options: self.parquet_options.clone(),
            default_values: self.default_values.clone(),
            default_adapter,
            logical_to_physical_names,
            original_physical_schema,
        }))
    }
}

/// Spark-compatible physical expression adapter.
///
/// Created by [`SparkPhysicalExprAdapterFactory::create`]. Rewrites
/// expressions at planning time to:
///
/// 1. replace references to missing columns with default values (or NULLs),
/// 2. apply Spark-compatible type-promotion rejection rules,
/// 3. wrap nested-type casts with [`SparkCastColumnExpr`] for Spark-compatible
///    conversion, and
/// 4. handle case-insensitive / field-id column matching.
#[derive(Debug)]
struct SparkPhysicalExprAdapter {
    /// The logical schema expected by the query.
    logical_file_schema: SchemaRef,
    /// The physical schema of the actual file being read (after remapping for
    /// field-id and case-insensitive matching).
    physical_file_schema: SchemaRef,
    /// Spark-specific options for type conversions.
    parquet_options: SparkParquetOptions,
    /// Default values for missing columns (keyed by `Column`).
    default_values: Option<HashMap<Column, ScalarValue>>,
    /// The default DataFusion adapter to delegate standard handling to.
    default_adapter: Arc<dyn PhysicalExprAdapter>,
    /// Mapping from logical column names to original physical column names,
    /// used in case-insensitive mode where names differ in casing. After the
    /// default adapter rewrites expressions using the remapped physical
    /// schema (with logical names), we need to restore the original physical
    /// names so that downstream code can find columns in the actual stream
    /// schema.
    logical_to_physical_names: Option<HashMap<String, String>>,
    /// The original (un-remapped) physical schema, kept for per-column
    /// duplicate detection in case-insensitive mode. Only set when
    /// `!case_sensitive`.
    original_physical_schema: Option<SchemaRef>,
}

impl PhysicalExprAdapter for SparkPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // In case-insensitive mode, check if any Column in this expression
        // references a field with multiple case-insensitive matches in the
        // physical schema. Only the columns actually referenced trigger the
        // error (not the whole schema).
        if let Some(orig_physical) = &self.original_physical_schema {
            let mut duplicate_err: Option<DataFusionError> = None;
            let _ = Arc::<dyn PhysicalExpr>::clone(&expr).transform(|e| {
                if let Some(col) = e.downcast_ref::<Column>()
                    && let Some((req, matched)) =
                        check_column_duplicate(col.name(), orig_physical)
                {
                    duplicate_err = Some(
                        ParquetSchemaError::DuplicateFieldCaseInsensitive {
                            required_field_name: req,
                            matched_fields: matched,
                        }
                        .into(),
                    );
                }
                Ok(Transformed::no(e))
            });
            if let Some(err) = duplicate_err {
                return Err(err);
            }
        }

        // First let the default adapter handle column remapping, missing
        // columns, and simple scalar type casts. Then replace DataFusion's
        // CastExpr (when it wraps a Column reference, i.e. came from the
        // default adapter's schema-mismatch handling) with either a
        // SparkCastColumnExpr (for nested types) or kept as-is for primitives,
        // applying the rejection rules first.
        //
        // The default adapter may fail for complex nested type casts (List,
        // Map). In that case, fall back to wrapping everything ourselves.
        let expr = self.replace_missing_with_defaults(expr)?;
        let expr = match self.default_adapter.rewrite(Arc::clone(&expr)) {
            Ok(rewritten) => rewritten
                .transform(|e| self.handle_schema_mismatch_cast(e))
                .data()?,
            Err(e) => {
                // Default adapter failed (likely complex nested type cast).
                // Handle all type mismatches ourselves using
                // `spark_parquet_convert`.
                log::debug!("Default schema adapter error: {e}");
                self.wrap_all_type_mismatches(expr)?
            }
        };

        // For case-insensitive mode: remap column names from logical back to
        // original physical names. The default adapter was given a remapped
        // physical schema (with logical names) so it could find columns. But
        // downstream code looks up columns by name in the actual parquet
        // stream schema, which uses the original physical names.
        let expr = if let Some(name_map) = &self.logical_to_physical_names {
            expr.transform(|e| {
                if let Some(col) = e.downcast_ref::<Column>()
                    && let Some(physical_name) = name_map.get(col.name())
                {
                    return Ok(Transformed::yes(Arc::new(Column::new(
                        physical_name,
                        col.index(),
                    ))));
                }
                Ok(Transformed::no(e))
            })
            .data()?
        } else {
            expr
        };

        Ok(expr)
    }
}

impl SparkPhysicalExprAdapter {
    /// Wrap ALL Column expressions that have type mismatches with
    /// [`SparkCastColumnExpr`]. This is the fallback path when the default
    /// adapter fails (e.g., for complex nested type casts like List<Struct>
    /// or Map). Uses [`spark_parquet_convert`] under the hood for the actual
    /// type conversion.
    ///
    /// [`spark_parquet_convert`]: super::parquet_support::spark_parquet_convert
    fn wrap_all_type_mismatches(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            if let Some(column) = e.downcast_ref::<Column>() {
                let col_name = column.name();

                // Resolve fields by name because this is the fallback path
                // that runs on the original expression when the default
                // adapter fails. The original expression was built against
                // the required (pruned) schema, so column indices refer to
                // that schema — not the logical or physical file schemas.
                // DataFusion's `DefaultPhysicalExprAdapter::resolve_physical_column`
                // also resolves by name for the same reason.
                let logical_field = if self.parquet_options.case_sensitive {
                    self.logical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name() == col_name)
                } else {
                    self.logical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name().eq_ignore_ascii_case(col_name))
                };
                let physical_field = if self.parquet_options.case_sensitive {
                    self.physical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name() == col_name)
                } else {
                    self.physical_file_schema
                        .fields()
                        .iter()
                        .find(|f| f.name().eq_ignore_ascii_case(col_name))
                };

                // Remap the column index to the physical file schema so
                // downstream evaluation reads the correct column from the
                // parquet batch.
                let physical_index = if self.parquet_options.case_sensitive {
                    self.physical_file_schema.index_of(col_name).ok()
                } else {
                    self.physical_file_schema
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(col_name))
                };

                if let (Some(logical_field), Some(physical_field), Some(phys_idx)) =
                    (logical_field, physical_field, physical_index)
                {
                    let remapped: Arc<dyn PhysicalExpr> = if column.index() != phys_idx {
                        Arc::new(Column::new(col_name, phys_idx))
                    } else {
                        Arc::clone(&e)
                    };

                    if logical_field.data_type() != physical_field.data_type() {
                        // Mirror the same string/binary -> non-string/binary
                        // rejection in `handle_schema_mismatch_cast`; this
                        // branch is reached when the default adapter rejected
                        // the cast and we'd otherwise build a
                        // SparkCastColumnExpr that can't actually convert
                        // (e.g. BINARY -> DECIMAL with no
                        // `DecimalLogicalTypeAnnotation`).
                        let physical_type = physical_field.data_type();
                        let target_type = logical_field.data_type();
                        if is_string_or_binary(physical_type)
                            && !is_string_or_binary(target_type)
                        {
                            return Err(parquet_schema_convert_err(
                                physical_field.name(),
                                physical_type,
                                target_type,
                            ));
                        }

                        let cast_expr: Arc<dyn PhysicalExpr> = Arc::new(
                            SparkCastColumnExpr::new(
                                remapped,
                                Arc::clone(physical_field),
                                Arc::clone(logical_field),
                                None,
                            )
                            .with_parquet_options(self.parquet_options.clone()),
                        );
                        return Ok(Transformed::yes(cast_expr));
                    } else if column.index() != phys_idx {
                        return Ok(Transformed::yes(remapped));
                    }
                }
            }
            Ok(Transformed::no(e))
        })
        .data()
    }

    /// Handle a `CastExpr` produced by the default adapter (which wraps a
    /// `Column` reference whose physical type differs from the logical type).
    /// Apply Spark's rejection rules first, then either wrap with
    /// [`SparkCastColumnExpr`] (for nested types) or leave as-is (for
    /// primitive scalar casts).
    fn handle_schema_mismatch_cast(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        let Some(cast) = expr.downcast_ref::<CastExpr>() else {
            return Ok(Transformed::no(expr));
        };

        // Only act on casts whose inner expression is a Column reference
        // produced by the default adapter for schema-mismatch handling.
        let Some(inner_column) = cast.expr().downcast_ref::<Column>() else {
            return Ok(Transformed::no(Arc::clone(&expr)));
        };

        // Look up the physical field by the column name in the physical
        // schema. This is the input to the cast.
        let Ok(physical_field) = self
            .physical_file_schema
            .field_with_name(inner_column.name())
        else {
            return Ok(Transformed::no(Arc::clone(&expr)));
        };

        let physical_type = physical_field.data_type();
        let target_field = cast.target_field();
        let target_type = target_field.data_type();
        let column_name = physical_field.name();

        // Reject reading a string/binary Parquet column as anything else.
        // Spark's `ParquetVectorUpdaterFactory.getUpdater` BINARY case allows
        // StringType / BinaryType, or DecimalType only when the column carries
        // a `DecimalLogicalTypeAnnotation` (which arrow-rs surfaces as
        // `Decimal128`, not `Binary`). Without this guard, runtime cast paths
        // silently return nulls, parse strings, or surface as a generic
        // Arrow type-mismatch error.
        if is_string_or_binary(physical_type) && !is_string_or_binary(target_type) {
            return Err(parquet_schema_convert_err(
                column_name,
                physical_type,
                target_type,
            ));
        }

        // Reject reading a primitive numeric Parquet column as StringType /
        // BinaryType. Spark has no `int -> string` etc. updater. Defer to
        // runtime via `RejectOnNonEmpty` so empty Parquet files (SPARK-26709)
        // pass and the JVM shim translates to
        // `SchemaColumnConvertNotSupportedException`.
        let physical_is_primitive_numeric = matches!(
            physical_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        );
        if physical_is_primitive_numeric && is_string_or_binary(target_type) {
            let rejection = reject_on_non_empty_expr(
                Arc::clone(cast.expr()),
                target_field,
                column_name,
                physical_type,
                target_type,
            );
            return Ok(Transformed::yes(rejection));
        }

        // Decimal-to-decimal narrowing. Spark's `isDecimalTypeMatched` (the
        // `DecimalLogicalTypeAnnotation` branch) allows the read only when
        //   `dst_scale >= src_scale` AND
        //   `dst_precision - dst_scale >= src_precision - src_scale`.
        // Either failure means silently dropping fractional digits or losing
        // integer-side magnitude.
        if let (DataType::Decimal128(src_p, src_s), DataType::Decimal128(dst_p, dst_s)) =
            (physical_type, target_type)
        {
            let src_int_precision = i32::from(*src_p) - i32::from(*src_s);
            let dst_int_precision = i32::from(*dst_p) - i32::from(*dst_s);
            if dst_s < src_s || dst_int_precision < src_int_precision {
                return Err(parquet_schema_convert_err(
                    column_name,
                    physical_type,
                    target_type,
                ));
            }
        }

        // Integer-to-decimal narrowing. Spark's `canReadAsDecimal` requires
        // `precision - scale >= 10` for an INT32 source and `>= 20` for INT64.
        // Unconditional in all Spark versions, so reject at plan time.
        let int_decimal_min_int_precision = match physical_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(10i32),
            DataType::Int64 => Some(20i32),
            _ => None,
        };
        if let Some(min_int_precision) = int_decimal_min_int_precision {
            let dst_precision_scale = match target_type {
                DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Some((*p, *s)),
                _ => None,
            };
            if let Some((dst_p, dst_s)) = dst_precision_scale {
                let dst_int_precision = i32::from(dst_p) - i32::from(dst_s);
                if dst_int_precision < min_int_precision {
                    return Err(parquet_schema_convert_err(
                        column_name,
                        physical_type,
                        target_type,
                    ));
                }
            }
        }

        // Type promotion (widening). When `allow_type_promotion` is false,
        // reject the three widenings (INT32→INT64, FLOAT→DOUBLE, INT32→DOUBLE)
        // that Spark 3.x's vectorized reader rejects. The flag tracks Spark's
        // per-version vectorized-reader policy. Deferred to runtime so empty
        // files (SPARK-26709) pass.
        if !self.parquet_options.allow_type_promotion {
            let is_disallowed_promotion = matches!(
                (physical_type, target_type),
                (DataType::Int32, DataType::Int64)
                    | (DataType::Float32, DataType::Float64)
                    | (DataType::Int32, DataType::Float64)
            );
            if is_disallowed_promotion {
                let rejection = reject_on_non_empty_expr(
                    Arc::clone(cast.expr()),
                    target_field,
                    column_name,
                    physical_type,
                    target_type,
                );
                return Ok(Transformed::yes(rejection));
            }
        }

        // Reject primitive Parquet conversions Spark's vectorized reader
        // rejects on every supported version (no matching branch in
        // `ParquetVectorUpdaterFactory.getUpdater`):
        //
        //   - `INT64 -> Int*` truncates lower bits.
        //   - `INT64 -> Float*` and `INT32 -> Float32` lose precision.
        //   - `Float* -> Int*` and `Float64 -> Float32` truncate / overflow.
        //   - `INT32 -> Timestamp` / `INT64 -> Date32` / `INT64 -> Timestamp`:
        //     date/timestamp-annotated columns surface as Date32 / Timestamp,
        //     so reaching this branch means the column was un-annotated.
        //   - `Date32 -> Timestamp(LTZ)`: Spark only allows
        //     Date -> TimestampNTZ.
        //   - `Timestamp -> Date32`: no Timestamp updater branches into Date.
        //
        // Deferred to runtime (SPARK-26709).
        let is_spark_rejected_conversion = matches!(
            (physical_type, target_type),
            // Long -> narrower int.
            (
                DataType::Int64,
                DataType::Int8 | DataType::Int16 | DataType::Int32,
            )
            // Long -> floating point.
            | (DataType::Int64, DataType::Float32 | DataType::Float64)
            // Long -> date / timestamp (raw INT64; annotated columns
            // surface as Date32/Timestamp).
            | (DataType::Int64, DataType::Date32)
            | (DataType::Int64, DataType::Timestamp(_, _))
            // Int -> float (DoubleType is allowed via
            // IntegerToDoubleUpdater; FloatType is not).
            | (
                DataType::Int8 | DataType::Int16 | DataType::Int32,
                DataType::Float32,
            )
            // Int -> timestamp (raw INT32; DATE-annotated columns surface
            // as Date32).
            | (
                DataType::Int8 | DataType::Int16 | DataType::Int32,
                DataType::Timestamp(_, _),
            )
            // Float -> int / Double -> int (no integer branches under
            // FLOAT/DOUBLE).
            | (
                DataType::Float32 | DataType::Float64,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            )
            // Double -> float (narrowing).
            | (DataType::Float64, DataType::Float32)
            // Date -> Timestamp(LTZ). Spark allows Date -> TimestampNTZ
            // only.
            | (DataType::Date32, DataType::Timestamp(_, Some(_)))
            // Timestamp -> Date.
            | (DataType::Timestamp(_, _), DataType::Date32)
        );
        if is_spark_rejected_conversion {
            let rejection = reject_on_non_empty_expr(
                Arc::clone(cast.expr()),
                target_field,
                column_name,
                physical_type,
                target_type,
            );
            return Ok(Transformed::yes(rejection));
        }

        // Scalar/complex mismatch (e.g. TIMESTAMP read as ARRAY<TIMESTAMP>):
        // Spark's vectorized reader rejects with
        // SchemaColumnConvertNotSupportedException (SPARK-45604). Same-shape
        // complex pairs and timestamp→timestamp / timestamp→int64 fall through
        // to SparkCastColumnExpr below.
        let is_complex = |t: &DataType| {
            matches!(
                t,
                DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
            )
        };
        if is_complex(physical_type) != is_complex(target_type) {
            return Err(parquet_schema_convert_err(
                column_name,
                physical_type,
                target_type,
            ));
        }

        // Same-shape complex casts, timestamp tz relabel (e.g.
        // Timestamp(us, None) -> Timestamp(us, Some("UTC")) for INT96 reads),
        // and Timestamp -> Int64 (Spark's `nanosAsLong`) need
        // `spark_parquet_convert`: it handles nested field selection,
        // metadata-only tz changes, and raw-value reinterpretation that
        // Spark's Cast would otherwise convert incorrectly.
        if matches!(
            (physical_type, target_type),
            (DataType::Struct(_), DataType::Struct(_))
                | (DataType::List(_), DataType::List(_))
                | (DataType::Map(_, _), DataType::Map(_, _))
                | (DataType::Timestamp(_, _), DataType::Timestamp(_, _))
                | (DataType::Timestamp(_, _), DataType::Int64)
        ) {
            let spark_cast: Arc<dyn PhysicalExpr> = Arc::new(
                SparkCastColumnExpr::new(
                    Arc::clone(cast.expr()),
                    Arc::new(physical_field.clone()),
                    Arc::clone(target_field),
                    None,
                )
                .with_parquet_options(self.parquet_options.clone()),
            );
            return Ok(Transformed::yes(spark_cast));
        }

        // For simple scalar type casts, leave the default-adapter-produced
        // CastExpr in place. Future work can add a Spark-specific Cast
        // PhysicalExpr to handle ANSI/Legacy mode differences for primitive
        // casts; today DataFusion's CastExpr is used for those.
        Ok(Transformed::no(expr))
    }

    /// Replace references to missing columns with default values.
    fn replace_missing_with_defaults(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let Some(defaults) = &self.default_values else {
            return Ok(expr);
        };

        if defaults.is_empty() {
            return Ok(expr);
        }

        // Build owned (column_name, default_value) pairs for columns missing
        // from the physical file. For each default: filter to only columns
        // absent from the physical schema, then type-cast the value to match
        // the logical schema's field type if they differ (using Spark cast
        // semantics).
        let missing_column_defaults: Vec<(String, ScalarValue)> = defaults
            .iter()
            .filter_map(|(col, val)| {
                let col_name = col.name();

                // Only include defaults for columns missing from the physical
                // file schema.
                let is_missing = if self.parquet_options.case_sensitive {
                    self.physical_file_schema.field_with_name(col_name).is_err()
                } else {
                    !self
                        .physical_file_schema
                        .fields()
                        .iter()
                        .any(|f| f.name().eq_ignore_ascii_case(col_name))
                };

                if !is_missing {
                    return None;
                }

                // Cast value to logical schema type if needed (only if types
                // differ).
                let value = self
                    .logical_file_schema
                    .field_with_name(col_name)
                    .ok()
                    .filter(|field| val.data_type() != *field.data_type())
                    .and_then(|field| {
                        super::parquet_support::spark_parquet_convert(
                            ColumnarValue::Scalar(val.clone()),
                            field.data_type(),
                            &self.parquet_options,
                        )
                        .ok()
                        .and_then(|cv| match cv {
                            ColumnarValue::Scalar(s) => Some(s),
                            _ => None,
                        })
                    })
                    .unwrap_or_else(|| val.clone());

                Some((col_name.to_string(), value))
            })
            .collect();

        let name_based: HashMap<&str, &ScalarValue> = missing_column_defaults
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect();

        if name_based.is_empty() {
            return Ok(expr);
        }

        replace_columns_with_literals(expr, &name_based)
    }
}

/// Defers a Parquet type-promotion rejection to runtime: returns an empty
/// array when the input batch has no rows, and raises
/// [`ParquetSchemaError::SchemaConvert`] otherwise.
///
/// Mirrors Spark's vectorized reader, which only invokes
/// `ParquetVectorUpdaterFactory.getUpdater` while decoding a row group. A
/// Parquet file with no row groups (e.g. one written from an empty DataFrame)
/// never triggers the per-row-group check, so a partition mixing such a file
/// with another whose schema would otherwise fail the type-promotion check
/// (SPARK-26709) is still readable.
#[derive(Debug, Eq)]
struct RejectOnNonEmpty {
    child: Arc<dyn PhysicalExpr>,
    target_field: FieldRef,
    column: String,
    physical_type: String,
    spark_type: String,
}

impl PartialEq for RejectOnNonEmpty {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.target_field.eq(&other.target_field)
            && self.column == other.column
            && self.physical_type == other.physical_type
            && self.spark_type == other.spark_type
    }
}

impl Hash for RejectOnNonEmpty {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.target_field.hash(state);
        self.column.hash(state);
        self.physical_type.hash(state);
        self.spark_type.hash(state);
    }
}

impl Display for RejectOnNonEmpty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "REJECT_PARQUET_TYPE_PROMOTION({} AS {})",
            self.column, self.spark_type
        )
    }
}

impl PhysicalExpr for RejectOnNonEmpty {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.target_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.target_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if batch.num_rows() == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                self.target_field.data_type(),
            )));
        }
        Err(ParquetSchemaError::SchemaConvert {
            file_path: String::new(),
            column: self.column.clone(),
            physical_type: self.physical_type.clone(),
            spark_type: self.spark_type.clone(),
        }
        .into())
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(RejectOnNonEmpty {
            child: children.pop().expect("child"),
            target_field: Arc::clone(&self.target_field),
            column: self.column.clone(),
            physical_type: self.physical_type.clone(),
            spark_type: self.spark_type.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::{File, create_dir_all};
    use std::path::PathBuf;

    use arrow::array::{
        Array, BinaryArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt32Array,
    };
    use arrow::datatypes::{Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;

    use crate::parquet::options::EvalMode;

    fn temp_parquet_path() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push("datafusion-spark-tests");
        create_dir_all(&path).unwrap();
        path.push(format!("schema-adapter-{}.parquet", rand::random::<u64>()));
        path
    }

    /// Create a Parquet file containing a single batch and read it back using
    /// the specified `required_schema` plus a [`SparkPhysicalExprAdapterFactory`].
    /// Returns the first batch read from the stream (or the first error).
    async fn roundtrip(
        batch: &RecordBatch,
        required_schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.allow_cast_unsigned_ints = true;
        roundtrip_with_options(batch, required_schema, options).await
    }

    async fn roundtrip_with_options(
        batch: &RecordBatch,
        required_schema: SchemaRef,
        options: SparkParquetOptions,
    ) -> Result<RecordBatch> {
        let path = temp_parquet_path();
        let path_str = path.to_str().unwrap().to_string();
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();

        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(SparkPhysicalExprAdapterFactory::new(options, None));

        let parquet_source = ParquetSource::new(required_schema);
        let files = FileGroup::new(vec![PartitionedFile::from_path(path_str)?]);
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(parquet_source),
        )
        .with_file_groups(vec![files])
        .with_expr_adapter(Some(factory))
        .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        let mut stream = parquet_exec.execute(0, Arc::new(TaskContext::default()))?;
        stream.next().await.unwrap()
    }

    /// Helper for the type-conversion rejection tests: write a 1-row batch and
    /// assert that reading it under `read_type` raises `ParquetSchemaConvert`.
    async fn assert_rejected_conversion(
        file_field: Field,
        values: Arc<dyn Array>,
        read_type: DataType,
    ) -> Result<String> {
        let file_schema = Arc::new(Schema::new(vec![file_field.clone()]));
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let read_field_name = file_schema.field(0).name();
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            read_field_name,
            read_type,
            false,
        )]));
        let err = roundtrip(&batch, required_schema)
            .await
            .expect_err("expected ParquetSchemaConvert");
        Ok(err.to_string())
    }

    /// Reading a non-BINARY Parquet column as `StringType` must raise the same
    /// `_LEGACY_ERROR_TEMP_2063`-shaped error as Spark's vectorized reader.
    #[tokio::test]
    async fn parquet_int_read_as_string_errors() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Utf8,
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: string")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// BINARY (string physical) read as IntegerType must raise the
    /// Spark-compatible error.
    #[tokio::test]
    async fn parquet_string_read_as_int_errors() -> Result<()> {
        let values = Arc::new(StringArray::from(vec!["bcd", "efg"])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Utf8, false),
            values,
            DataType::Int32,
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: int")
                && msg.contains("Found: BINARY"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Reading a plain BINARY Parquet column as `DecimalType` must raise a
    /// `ParquetSchemaConvert`-shaped error.
    #[tokio::test]
    async fn parquet_binary_read_as_decimal_errors() -> Result<()> {
        let values =
            Arc::new(BinaryArray::from_vec(vec![b"1.2", b"3.4"])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Binary, false),
            values,
            DataType::Decimal128(37, 1),
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: decimal(37,1)")
                && msg.contains("Found: BINARY"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// INT32 -> Decimal where `precision - scale < 10` (the minimum that can
    /// represent the full INT32 range).
    #[tokio::test]
    async fn parquet_int32_read_as_narrow_decimal_errors() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Decimal128(9, 0),
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: decimal")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// INT32 -> Decimal where `precision - scale >= 10` is allowed.
    #[tokio::test]
    async fn parquet_int32_read_as_wide_decimal_succeeds() -> Result<()> {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(10, 0),
            false,
        )]));
        let _ = roundtrip(&batch, required_schema).await?;
        Ok(())
    }

    /// INT64 -> Decimal where `precision - scale < 20`.
    #[tokio::test]
    async fn parquet_int64_read_as_narrow_decimal_errors() -> Result<()> {
        let values = Arc::new(Int64Array::from(vec![1i64, 2, 3])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Decimal128(19, 0),
        )
        .await?;
        assert!(
            msg.contains("Column: [[a]]")
                && msg.contains("Expected: decimal")
                && msg.contains("Found: INT64"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Reading Decimal(P, S) as Decimal(P', S) where P' < P must raise the
    /// Spark-compatible error.
    #[tokio::test]
    async fn parquet_decimal_precision_narrowing_errors() -> Result<()> {
        let batch = decimal_batch(10, 2)?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(5, 2),
            false,
        )]));

        let err = roundtrip(&batch, required_schema).await.expect_err(
            "expected ParquetSchemaConvert for Decimal(10, 2) -> Decimal(5, 2)",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("Column: [[a]]") && msg.contains("Expected: decimal(5,2)"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Reading Decimal(P, S) as Decimal(P', S') where the integer-precision
    /// `P - S` shrinks must raise the Spark-compatible error.
    #[tokio::test]
    async fn parquet_decimal_int_precision_narrowing_errors() -> Result<()> {
        let batch = decimal_batch(10, 4)?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(5, 2),
            false,
        )]));

        let err = roundtrip(&batch, required_schema).await.expect_err(
            "expected ParquetSchemaConvert for Decimal(10, 4) -> Decimal(5, 2)",
        );
        let msg = err.to_string();
        assert!(msg.contains("Column: [[a]]"), "unexpected error: {msg}");
        Ok(())
    }

    /// Sanity check: widening both precision and scale by the same amount is
    /// allowed (the cast is lossless).
    #[tokio::test]
    async fn parquet_decimal_widening_succeeds() -> Result<()> {
        let batch = decimal_batch(5, 2)?;
        let required_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(7, 4),
            false,
        )]));
        let _ = roundtrip(&batch, required_schema).await?;
        Ok(())
    }

    fn decimal_batch(precision: u8, scale: i8) -> Result<RecordBatch> {
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(precision, scale),
            false,
        )]));
        let values = Arc::new(
            Decimal128Array::from(vec![123i128, 456])
                .with_precision_and_scale(precision, scale)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        ) as Arc<dyn Array>;
        Ok(RecordBatch::try_new(file_schema, vec![values])?)
    }

    /// `INT64 -> INT32` truncates to the lower 32 bits in DataFusion's cast.
    /// Spark's vectorized reader rejects this.
    #[tokio::test]
    async fn parquet_long_read_as_int_errors() -> Result<()> {
        let values = Arc::new(Int64Array::from(vec![1i64, 1 << 33])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Int32,
        )
        .await?;
        assert!(
            msg.contains("Found: INT64") && msg.contains("Expected: int"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `INT64 -> Float64` loses precision for large values; Spark rejects.
    #[tokio::test]
    async fn parquet_long_read_as_double_errors() -> Result<()> {
        let values =
            Arc::new(Int64Array::from(vec![1i64, (1i64 << 54) + 1])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Float64,
        )
        .await?;
        assert!(
            msg.contains("Found: INT64") && msg.contains("Expected: double"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Float64 -> Float32` overflows / loses precision; Spark rejects.
    #[tokio::test]
    async fn parquet_double_read_as_float_errors() -> Result<()> {
        let values = Arc::new(Float64Array::from(vec![1.5_f64, 1e40])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Float64, false),
            values,
            DataType::Float32,
        )
        .await?;
        assert!(
            msg.contains("Found: DOUBLE") && msg.contains("Expected: float"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Float32 -> Int64` truncates the fractional part; Spark rejects.
    #[tokio::test]
    async fn parquet_float_read_as_long_errors() -> Result<()> {
        let values = Arc::new(Float32Array::from(vec![1.5_f32, 2.5])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Float32, false),
            values,
            DataType::Int64,
        )
        .await?;
        assert!(
            msg.contains("Found: FLOAT") && msg.contains("Expected: bigint"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Int32 -> Float32` loses precision for values past `2^24`; Spark rejects.
    #[tokio::test]
    async fn parquet_int_read_as_float_errors() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![1, (1 << 25) + 1])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Float32,
        )
        .await?;
        assert!(
            msg.contains("Found: INT32") && msg.contains("Expected: float"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Int32 -> Timestamp(_, None)`: raw INT32 reinterpreted as epoch seconds
    /// produces dates near the Unix epoch. Only DATE-annotated INT32 columns
    /// (which surface as `Date32`) are allowed to read as `TimestampNTZ`.
    #[tokio::test]
    async fn parquet_int_read_as_timestamp_ntz_errors() -> Result<()> {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int32, false),
            values,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .await?;
        assert!(
            msg.contains("Found: INT32") && msg.contains("Expected: timestamp"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Int64 -> Date32`: raw INT64 (no DATE annotation, otherwise the file
    /// would surface as `Date32`).
    #[tokio::test]
    async fn parquet_long_read_as_date_errors() -> Result<()> {
        let values = Arc::new(Int64Array::from(vec![1i64, 2])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Int64, false),
            values,
            DataType::Date32,
        )
        .await?;
        assert!(
            msg.contains("Found: INT64") && msg.contains("Expected: date"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Date32 -> Timestamp(_, Some(_))` (LTZ). Spark allows
    /// `Date -> TimestampNTZ` only.
    #[tokio::test]
    async fn parquet_date_read_as_ltz_timestamp_errors() -> Result<()> {
        let values = Arc::new(Date32Array::from(vec![18262, 18263])) as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Date32, false),
            values,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        )
        .await?;
        assert!(
            msg.contains("Found: INT32") && msg.contains("Expected: timestamp"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// `Timestamp(_, _) -> Date32`: no Timestamp updater branches into
    /// `DateType` in Spark.
    #[tokio::test]
    async fn parquet_timestamp_read_as_date_errors() -> Result<()> {
        let values = Arc::new(TimestampMicrosecondArray::from(vec![0i64, 1_000_000]))
            as Arc<dyn Array>;
        let msg = assert_rejected_conversion(
            Field::new("a", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            values,
            DataType::Date32,
        )
        .await?;
        assert!(msg.contains("Expected: date"), "unexpected error: {msg}");
        Ok(())
    }

    /// SPARK-26709: an empty Parquet file with a column that would otherwise
    /// fail the type-promotion check (INT32 read as INT64 when
    /// `allow_type_promotion` is false) must still be readable. Spark's
    /// vectorized reader only enforces the check per row group, so a file
    /// with no row groups passes silently.
    #[tokio::test]
    async fn parquet_empty_file_disallowed_widening() -> Result<()> {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let path = temp_parquet_path();
        let path_str = path.to_str().unwrap().to_string();
        let file = File::create(&path)?;
        let writer = ArrowWriter::try_new(file, Arc::clone(&file_schema), None).unwrap();
        writer.close().unwrap();

        let required_schema =
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));

        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.allow_type_promotion = false;

        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(SparkPhysicalExprAdapterFactory::new(options, None));

        let parquet_source = ParquetSource::new(required_schema);
        let files = FileGroup::new(vec![PartitionedFile::from_path(path_str)?]);
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(parquet_source),
        )
        .with_file_groups(vec![files])
        .with_expr_adapter(Some(factory))
        .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        let mut stream = parquet_exec.execute(0, Arc::new(TaskContext::default()))?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            assert_eq!(batch.num_rows(), 0);
        }
        Ok(())
    }

    /// Companion: a non-empty file with the same widening must raise
    /// `ParquetSchemaConvert` at runtime (deferred from plan time).
    #[tokio::test]
    async fn parquet_non_empty_file_disallowed_widening_errors() -> Result<()> {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![values])?;

        let path = temp_parquet_path();
        let path_str = path.to_str().unwrap().to_string();
        let file = File::create(&path)?;
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&file_schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let required_schema =
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));

        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.allow_type_promotion = false;

        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(SparkPhysicalExprAdapterFactory::new(options, None));

        let parquet_source = ParquetSource::new(required_schema);
        let files = FileGroup::new(vec![PartitionedFile::from_path(path_str)?]);
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(parquet_source),
        )
        .with_file_groups(vec![files])
        .with_expr_adapter(Some(factory))
        .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        let mut stream = parquet_exec.execute(0, Arc::new(TaskContext::default()))?;
        let first = stream.next().await.unwrap();
        let err =
            first.expect_err("expected ParquetSchemaConvert error on non-empty file");
        let msg = err.to_string();
        assert!(
            msg.contains("Column: [[col]]")
                && msg.contains("Expected: bigint")
                && msg.contains("Found: INT32"),
            "unexpected error: {msg}"
        );
        Ok(())
    }

    /// Roundtrip an unsigned integer column read as a signed integer of the
    /// same width (Iceberg / Arrow files commonly use unsigned types where
    /// Spark expects signed).
    #[tokio::test]
    async fn parquet_roundtrip_unsigned_int() -> Result<()> {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));

        let ids = Arc::new(UInt32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids])?;

        let required_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let _ = roundtrip(&batch, required_schema).await?;
        Ok(())
    }

    /// Reading `b` from a file that contains `A`, `B`, and `b` in
    /// case-insensitive mode must raise the `_LEGACY_ERROR_TEMP_2093`-shaped
    /// duplicate-field error.
    #[tokio::test]
    async fn parquet_duplicate_fields_case_insensitive() -> Result<()> {
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("A", DataType::Int32, false),
            Field::new("B", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let col_a = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let col_b1 = Arc::new(Int32Array::from(vec![4, 5, 6])) as Arc<dyn Array>;
        let col_b2 = Arc::new(Int32Array::from(vec![7, 8, 9])) as Arc<dyn Array>;
        let batch =
            RecordBatch::try_new(Arc::clone(&file_schema), vec![col_a, col_b1, col_b2])?;

        let path = temp_parquet_path();
        let path_str = path.to_str().unwrap().to_string();
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let required_schema =
            Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));

        let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        options.case_sensitive = false;

        let factory: Arc<dyn PhysicalExprAdapterFactory> =
            Arc::new(SparkPhysicalExprAdapterFactory::new(options, None));

        let parquet_source = ParquetSource::new(required_schema);
        let files = FileGroup::new(vec![PartitionedFile::from_path(path_str)?]);
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(parquet_source),
        )
        .with_file_groups(vec![files])
        .with_expr_adapter(Some(factory))
        .build();

        let parquet_exec = DataSourceExec::new(Arc::new(file_scan_config));
        let mut stream = parquet_exec.execute(0, Arc::new(TaskContext::default()))?;
        let result = stream.next().await.unwrap();

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Found duplicate field"),
            "expected duplicate field error, got: {err_msg}"
        );
        Ok(())
    }
}
