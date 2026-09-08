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
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::physical_expr::PhysicalExpr;

use arrow::compute::{CastOptions, can_cast_types};
use arrow::datatypes::{DataType, DataType::*, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatterFactory, DurationFormat, FormatOptions};
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion_common::datatype::DataTypeExt;
use datafusion_common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion_common::nested_struct::{
    requires_nested_struct_cast, validate_data_type_compatibility,
};
use datafusion_common::{Result, not_impl_err};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;

const DEFAULT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

const DEFAULT_SAFE_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

/// Owns Arrow's borrowed format strings so protobuf decoding does not leak them.
#[derive(Debug, Clone)]
struct OwnedCastOptions {
    safe: bool,
    format_safe: bool,
    null: String,
    date_format: Option<String>,
    datetime_format: Option<String>,
    timestamp_format: Option<String>,
    timestamp_tz_format: Option<String>,
    time_format: Option<String>,
    duration_format: DurationFormat,
    types_info: bool,
    quoted_strings: bool,
    formatter_factory: Option<&'static dyn ArrayFormatterFactory>,
}

impl From<CastOptions<'static>> for OwnedCastOptions {
    fn from(options: CastOptions<'static>) -> Self {
        let CastOptions {
            safe,
            format_options,
        } = options;
        Self {
            safe,
            format_safe: format_options.safe(),
            null: format_options.null().to_owned(),
            date_format: format_options.date_format().map(str::to_owned),
            datetime_format: format_options.datetime_format().map(str::to_owned),
            timestamp_format: format_options.timestamp_format().map(str::to_owned),
            timestamp_tz_format: format_options.timestamp_tz_format().map(str::to_owned),
            time_format: format_options.time_format().map(str::to_owned),
            duration_format: format_options.duration_format(),
            types_info: format_options.types_info(),
            quoted_strings: format_options.quoted_strings(),
            formatter_factory: format_options.formatter_factory(),
        }
    }
}

impl OwnedCastOptions {
    fn as_arrow(&self) -> CastOptions<'_> {
        let Self {
            safe,
            format_safe,
            null,
            date_format,
            datetime_format,
            timestamp_format,
            timestamp_tz_format,
            time_format,
            duration_format,
            types_info,
            quoted_strings,
            formatter_factory,
        } = self;
        CastOptions {
            safe: *safe,
            format_options: FormatOptions::new()
                .with_display_error(*format_safe)
                .with_null(null)
                .with_date_format(date_format.as_deref())
                .with_datetime_format(datetime_format.as_deref())
                .with_timestamp_format(timestamp_format.as_deref())
                .with_timestamp_tz_format(timestamp_tz_format.as_deref())
                .with_time_format(time_format.as_deref())
                .with_duration_format(*duration_format)
                .with_types_info(*types_info)
                .with_quoted_strings(*quoted_strings)
                .with_formatter_factory(*formatter_factory),
        }
    }
}

impl PartialEq for OwnedCastOptions {
    fn eq(&self, other: &Self) -> bool {
        self.as_arrow() == other.as_arrow()
    }
}

impl Eq for OwnedCastOptions {}

impl Hash for OwnedCastOptions {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_arrow().hash(state);
    }
}

/// Check if name-based struct casting is allowed by validating field compatibility.
///
/// This function applies the same validation rules as execution time to ensure
/// planning-time validation matches runtime validation, enabling fail-fast behavior
/// instead of deferring errors to execution. Handles structs at any nesting level
/// (e.g., `List<Struct>`, `Dictionary<_, Struct>`).
fn can_cast_named_struct_types(source: &DataType, target: &DataType) -> bool {
    validate_data_type_compatibility("", source, target).is_ok()
}

/// CAST expression casts an expression to a specific data type and returns a runtime error on invalid cast
#[derive(Debug, Clone, Eq)]
pub struct CastExpr {
    /// The expression to cast
    pub expr: Arc<dyn PhysicalExpr>,
    /// The target field.
    ///
    /// For a type-only cast (see [`CastExpr::new`]) this is a field synthesized
    /// from the target data type alone and only its data type is meaningful.
    /// For a cast built from an explicit field (see
    /// [`CastExpr::new_with_target_field`]) its metadata and nullability are
    /// applied to the output field as-is.
    target_field: FieldRef,
    /// Whether `target_field` was supplied by the caller (as opposed to being
    /// synthesized from a `DataType`), and therefore whether its metadata and
    /// nullability describe the output field exactly.
    explicit_target: bool,
    /// Cast options
    cast_options: OwnedCastOptions,
}

// Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
impl PartialEq for CastExpr {
    fn eq(&self, other: &Self) -> bool {
        // Compare the semantically meaningful parts of the target field only:
        // the field name never affects the output of this expression.
        self.expr.eq(&other.expr)
            && self.cast_type().eq(other.cast_type())
            && self.target_metadata().eq(&other.target_metadata())
            && self.target_nullable().eq(&other.target_nullable())
            && self.cast_options.eq(&other.cast_options)
    }
}

impl Hash for CastExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.cast_type().hash(state);
        // Hash the metadata by iterating over sorted keys for deterministic ordering
        if let Some(metadata) = self.target_metadata() {
            let mut entries: Vec<_> = metadata.iter().collect();
            entries.sort_by_key(|(k, _)| *k);
            for (k, v) in entries {
                k.hash(state);
                v.hash(state);
            }
        }
        self.target_nullable().hash(state);
        self.cast_options.hash(state);
    }
}

impl CastExpr {
    /// Create a new `CastExpr` using only a `DataType`.
    ///
    /// This constructor creates a type-only cast where metadata and nullability
    /// are passed through from the source expression (with extension type keys
    /// stripped from metadata). This is the most common use case when you only
    /// need to change the data type.
    ///
    /// For explicit control over the output field's metadata and nullability,
    /// use [`CastExpr::new_with_target_field`] or the individual builder methods.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        cast_type: DataType,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            target_field: cast_type.into_nullable_field_ref(),
            explicit_target: false,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS).into(),
        }
    }

    /// Create a new `CastExpr` with an explicit target `FieldRef`.
    ///
    /// The provided `target_field` determines the output characteristics:
    /// - The field's data type becomes the cast target type
    /// - The field's metadata is used exactly as provided
    /// - The field's nullability is preserved
    ///
    /// This is the preferred constructor when the caller has explicit field
    /// information that should be used exactly (for example, during schema
    /// enforcement or adapter layers).
    ///
    /// See [`CastExpr::new`] for type-only casts where source metadata should
    /// pass through.
    pub fn new_with_target_field(
        expr: Arc<dyn PhysicalExpr>,
        target_field: FieldRef,
        cast_options: Option<CastOptions<'static>>,
    ) -> Self {
        Self {
            expr,
            target_field,
            explicit_target: true,
            cast_options: cast_options.unwrap_or(DEFAULT_CAST_OPTIONS).into(),
        }
    }

    /// The expression to cast
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// The data type to cast to
    pub fn cast_type(&self) -> &DataType {
        self.target_field.data_type()
    }

    /// Explicit metadata for the output field, or `None` to pass through source metadata.
    pub fn target_metadata(&self) -> Option<&HashMap<String, String>> {
        self.explicit_target.then(|| self.target_field.metadata())
    }

    /// Explicit nullability for the output field, or `None` to pass through source nullability.
    pub fn target_nullable(&self) -> Option<bool> {
        self.explicit_target
            .then(|| self.target_field.is_nullable())
    }

    /// The target field this cast was constructed with.
    ///
    /// For a type-only cast this is a field synthesized from the target data
    /// type alone; only its data type is meaningful. Note that the returned
    /// field may not match what `return_field()` returns when evaluated against
    /// a schema, since `return_field()` may incorporate source field information.
    ///
    /// Prefer [`cast_type()`], [`target_metadata()`], and [`target_nullable()`]
    /// for direct access to the individual components.
    ///
    /// [`cast_type()`]: CastExpr::cast_type
    /// [`target_metadata()`]: CastExpr::target_metadata
    /// [`target_nullable()`]: CastExpr::target_nullable
    pub fn target_field(&self) -> &FieldRef {
        &self.target_field
    }

    /// The cast options, borrowing custom format strings from this expression.
    pub fn cast_options(&self) -> CastOptions<'_> {
        self.cast_options.as_arrow()
    }

    /// Rebuild this cast with a new child and explicit target while preserving its options.
    pub fn with_new_expr_and_target_field(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        target_field: FieldRef,
    ) -> Self {
        Self {
            expr,
            target_field,
            explicit_target: true,
            cast_options: self.cast_options.clone(),
        }
    }

    /// Whether this cast has explicit metadata (vs pass-through from source).
    pub fn has_explicit_metadata(&self) -> bool {
        self.explicit_target
    }

    /// Whether this cast has explicit nullability (vs pass-through from source).
    pub fn has_explicit_nullability(&self) -> bool {
        self.explicit_target
    }

    fn resolved_target_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        // Try to get the source field for the name. If the target field is
        // explicit, we can fall back to an empty name if the source lookup fails
        // (e.g., for virtual row-index columns appended at scan time).
        let source_result = self.expr.return_field(input_schema);

        if self.explicit_target {
            // Metadata and nullability come from the target field verbatim
            let name = source_result
                .as_ref()
                .map(|f| f.name().to_string())
                .unwrap_or_default();
            return Ok(Arc::new(
                Field::new(
                    name,
                    self.cast_type().clone(),
                    self.target_field.is_nullable(),
                )
                .with_metadata(self.target_field.metadata().clone()),
            ));
        }

        // Type-only cast: pass through the source metadata and nullability,
        // stripping extension type keys (the cast is to a plain storage type).
        source_result.map(|source_field| {
            let mut metadata = source_field.metadata().clone();
            metadata.remove(EXTENSION_TYPE_NAME_KEY);
            metadata.remove(EXTENSION_TYPE_METADATA_KEY);

            Arc::new(
                source_field
                    .as_ref()
                    .clone()
                    .with_data_type(self.cast_type().clone())
                    .with_metadata(metadata),
            )
        })
    }

    /// Check if casting from the specified source type to the target type is a
    /// widening cast (e.g. from `Int8` to `Int16`).
    pub fn check_bigger_cast(cast_type: &DataType, src: &DataType) -> bool {
        if cast_type.eq(src) {
            return true;
        }
        matches!(
            (src, cast_type),
            (Int8, Int16 | Int32 | Int64)
                | (Int16, Int32 | Int64)
                | (Int32, Int64)
                | (UInt8, UInt16 | UInt32 | UInt64)
                | (UInt16, UInt32 | UInt64)
                | (UInt32, UInt64)
                | (Int8 | Int16 | UInt8 | UInt16, Float32)
                | (Int8 | Int16 | Int32 | UInt8 | UInt16 | UInt32, Float64)
                | (Utf8, LargeUtf8)
        )
    }

    /// Check if the cast is a widening cast (e.g. from `Int8` to `Int16`).
    pub fn is_bigger_cast(&self, src: &DataType) -> bool {
        Self::check_bigger_cast(self.cast_type(), src)
    }
}

pub(crate) fn is_order_preserving_cast_family(
    source_type: &DataType,
    target_type: &DataType,
) -> bool {
    (source_type.is_numeric() || *source_type == Boolean) && target_type.is_numeric()
        || source_type.is_temporal() && target_type.is_temporal()
        || source_type.eq(target_type)
}

pub(crate) fn cast_expr_properties(
    child: &ExprProperties,
    target_type: &DataType,
) -> Result<ExprProperties> {
    let unbounded = Interval::make_unbounded(target_type)?;
    let source_type = child.range.data_type();
    // A widening cast is additionally one-to-one, so it is strictly
    // order-preserving; a narrowing cast may collapse distinct values,
    // breaking the ordering of subsequent sort keys.
    let bigger_cast = CastExpr::check_bigger_cast(target_type, &source_type);
    if is_order_preserving_cast_family(&source_type, target_type) || bigger_cast {
        Ok(child
            .clone()
            .with_range(unbounded)
            .with_strictly_order_preserving(
                child.strictly_order_preserving && bigger_cast,
            ))
    } else {
        Ok(ExprProperties::new_unknown().with_range(unbounded))
    }
}

#[cfg(feature = "proto")]
fn serialize_cast_options(
    options: &OwnedCastOptions,
) -> Result<Option<datafusion_proto_models::protobuf::PhysicalCastOptions>> {
    use datafusion_proto_models::protobuf;

    let options = options.as_arrow();
    if options.format_options.formatter_factory().is_some() {
        return not_impl_err!(
            "CastExpr serialization does not support a custom formatter_factory"
        );
    }
    if options == DEFAULT_CAST_OPTIONS {
        return Ok(None);
    }

    let CastOptions {
        safe,
        format_options,
    } = options;
    let duration_format = match format_options.duration_format() {
        DurationFormat::ISO8601 => protobuf::PhysicalDurationFormat::Iso8601,
        DurationFormat::Pretty => protobuf::PhysicalDurationFormat::Pretty,
        _ => {
            return not_impl_err!(
                "CastExpr serialization does not support this duration format"
            );
        }
    };

    Ok(Some(protobuf::PhysicalCastOptions {
        safe,
        format_options: Some(protobuf::PhysicalFormatOptions {
            safe: format_options.safe(),
            null: format_options.null().to_owned(),
            date_format: format_options.date_format().map(str::to_owned),
            datetime_format: format_options.datetime_format().map(str::to_owned),
            timestamp_format: format_options.timestamp_format().map(str::to_owned),
            timestamp_tz_format: format_options.timestamp_tz_format().map(str::to_owned),
            time_format: format_options.time_format().map(str::to_owned),
            duration_format: duration_format.into(),
            types_info: format_options.types_info(),
            quoted_strings: format_options.quoted_strings(),
        }),
    }))
}

#[cfg(feature = "proto")]
fn deserialize_cast_options(
    options: Option<&datafusion_proto_models::protobuf::PhysicalCastOptions>,
) -> Result<OwnedCastOptions> {
    use datafusion_common::internal_datafusion_err;
    use datafusion_proto_models::protobuf;

    let Some(options) = options else {
        return Ok(DEFAULT_CAST_OPTIONS.into());
    };
    let protobuf::PhysicalCastOptions {
        safe,
        format_options,
    } = options;
    let format_options = format_options.as_ref().ok_or_else(|| {
        internal_datafusion_err!(
            "CastExpr cast_options is missing required field 'format_options'"
        )
    })?;
    let protobuf::PhysicalFormatOptions {
        safe: format_safe,
        null,
        date_format,
        datetime_format,
        timestamp_format,
        timestamp_tz_format,
        time_format,
        duration_format,
        types_info,
        quoted_strings,
    } = format_options;
    let duration_format = match protobuf::PhysicalDurationFormat::try_from(
        *duration_format,
    )
    .map_err(|_| {
        internal_datafusion_err!(
            "CastExpr has invalid duration format value {duration_format}"
        )
    })? {
        protobuf::PhysicalDurationFormat::Iso8601 => DurationFormat::ISO8601,
        protobuf::PhysicalDurationFormat::Pretty => DurationFormat::Pretty,
    };

    Ok(OwnedCastOptions {
        safe: *safe,
        format_safe: *format_safe,
        null: null.clone(),
        date_format: date_format.clone(),
        datetime_format: datetime_format.clone(),
        timestamp_format: timestamp_format.clone(),
        timestamp_tz_format: timestamp_tz_format.clone(),
        time_format: time_format.clone(),
        duration_format,
        types_info: *types_info,
        quoted_strings: *quoted_strings,
        // Runtime formatter factories have no protobuf representation; the
        // encoder rejects them rather than silently dropping behavior.
        formatter_factory: None,
    })
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST({} AS {})", self.expr, self.cast_type())
    }
}

impl PhysicalExpr for CastExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        // A cast is nullable if **either** the child is nullable or the
        // target field allows nulls.  This conservative rule prevents
        // optimizers from assuming a non-null result when a null input could
        // still propagate.  `return_field()` continues to expose the exact
        // target metadata separately.
        let child_nullable = self.expr.nullable(input_schema)?;
        let target_nullable = self.resolved_target_field(input_schema)?.is_nullable();
        Ok(child_nullable || target_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        let cast_options = self.cast_options();
        value
            .cast_to(self.cast_type(), Some(&cast_options))
            .map_err(|error| {
                let source = self
                    .expr
                    .return_field(batch.schema().as_ref())
                    .ok()
                    .filter(|field| !field.name().is_empty())
                    .map(|field| format!("field '{}'", field.name()))
                    .unwrap_or_else(|| format!("expression '{}'", self.expr));
                error.context(format!(
                    "Failed to cast {source} from {} to {}",
                    value.data_type(),
                    self.cast_type()
                ))
            })
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.resolved_target_field(input_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CastExpr {
            expr: Arc::clone(&children[0]),
            target_field: Arc::clone(&self.target_field),
            explicit_target: self.explicit_target,
            cast_options: self.cast_options.clone(),
        }))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        // Cast current node's interval to the right type:
        children[0].cast_to(self.cast_type(), &self.cast_options())
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let child_interval = children[0];
        // Get child's datatype:
        let cast_type = child_interval.data_type();
        Ok(Some(vec![
            interval.cast_to(&cast_type, &DEFAULT_SAFE_CAST_OPTIONS)?,
        ]))
    }

    /// A [`CastExpr`] preserves the ordering of its child if the cast is done
    /// under the same datatype family.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        cast_expr_properties(&children[0], self.cast_type())
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST(")?;
        self.expr.fmt_sql(f)?;
        write!(f, " AS {:?}", self.cast_type())?;

        write!(f, ")")
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalExprNode>> {
        use datafusion_proto_models::protobuf;

        let Self {
            expr,
            target_field,
            explicit_target,
            cast_options,
        } = self;
        // Presence carries `explicit_target`, even for a default-shaped field.
        let target_field_proto = if *explicit_target {
            Some(target_field.as_ref().try_into()?)
        } else {
            None
        };
        let cast_options_proto = serialize_cast_options(cast_options)?;

        Ok(Some(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::Cast(Box::new(
                protobuf::PhysicalCastNode {
                    expr: Some(Box::new(ctx.encode_child(expr)?)),
                    arrow_type: Some(target_field.data_type().try_into()?),
                    target_field: target_field_proto,
                    cast_options: cast_options_proto,
                },
            ))),
        }))
    }
}

#[cfg(feature = "proto")]
impl CastExpr {
    /// Reconstruct a [`CastExpr`] from its protobuf representation.
    ///
    /// Takes the whole [`PhysicalExprNode`] so the decode signature matches
    /// other migrated expressions and can inspect outer-node metadata if
    /// needed in the future.
    ///
    /// [`PhysicalExprNode`]: datafusion_proto_models::protobuf::PhysicalExprNode
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalExprNode,
        ctx: &datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx<'_>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion_common::internal_err;
        use datafusion_physical_expr_common::expect_expr_variant;
        use datafusion_physical_expr_common::physical_expr::proto_decode::require_proto_field;
        use datafusion_proto_models::protobuf;

        let cast_expr = expect_expr_variant!(
            node,
            protobuf::physical_expr_node::ExprType::Cast,
            "CastExpr",
        );
        let protobuf::PhysicalCastNode {
            expr,
            arrow_type,
            target_field,
            cast_options,
        } = &**cast_expr;
        let expr = ctx.decode_required_expression(expr.as_deref(), "CastExpr", "expr")?;
        let arrow_type =
            require_proto_field(arrow_type.as_ref(), "CastExpr", "arrow_type")?;
        let cast_type: DataType = arrow_type.try_into()?;
        let target_field = target_field
            .as_ref()
            .map(|field| {
                let field: Field = field.try_into()?;
                if field.data_type() != &cast_type {
                    return internal_err!(
                        "CastExpr target_field type does not match arrow_type"
                    );
                }
                Ok(Arc::new(field))
            })
            .transpose()?;
        let cast_options = deserialize_cast_options(cast_options.as_ref())?;
        let (target_field, explicit_target) = match target_field {
            Some(target_field) => (target_field, true),
            None => (cast_type.into_nullable_field_ref(), false),
        };

        Ok(Arc::new(CastExpr {
            expr,
            target_field,
            explicit_target,
            cast_options,
        }))
    }
}

/// Return a PhysicalExpression representing `expr` casted to
/// `cast_type`, if any casting is needed.
///
/// Note that such casts may lose type information
pub fn cast_with_options(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
    cast_options: Option<CastOptions<'static>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = expr.data_type(input_schema)?;

    // If the types match, no cast is needed for a type-only cast
    if expr_type == cast_type {
        return Ok(Arc::clone(&expr));
    }

    let can_build_cast = if requires_nested_struct_cast(&expr_type, &cast_type) {
        can_cast_named_struct_types(&expr_type, &cast_type)
    } else {
        can_cast_types(&expr_type, &cast_type)
    };

    if !can_build_cast {
        return not_impl_err!("Unsupported CAST from {expr_type} to {cast_type}");
    }

    Ok(Arc::new(CastExpr::new(expr, cast_type, cast_options)))
}

/// Return a PhysicalExpression representing `expr` casted to `target_field`,
/// preserving any explicit field semantics such as name, nullability, and
/// metadata.
///
/// If the input expression already has the same data type and the target field
/// has no explicit metadata or nullability constraints, the original expression
/// is returned unchanged.
pub fn cast_with_target_field(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    target_field: &FieldRef,
    cast_options: Option<CastOptions<'static>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = expr.data_type(input_schema)?;
    let cast_type = target_field.data_type();

    // Check if this is a "default" target field (type-only cast with no explicit
    // metadata or nullability constraints). This is the field created by
    // `into_nullable_field_ref()` when only a DataType is known.
    let is_type_only = target_field.name().is_empty()
        && target_field.is_nullable()
        && target_field.metadata().is_empty();

    // For same-type casts, we can skip creating a CastExpr only if:
    // 1. The target is type-only (no explicit metadata)
    // 2. The source has no extension metadata that needs to be stripped
    // Otherwise we need the CastExpr to strip extension metadata from the source.
    if expr_type == *cast_type && is_type_only {
        let source_field = expr.return_field(input_schema)?;
        let has_extension_metadata = source_field
            .metadata()
            .contains_key(EXTENSION_TYPE_NAME_KEY);
        if !has_extension_metadata {
            return Ok(Arc::clone(&expr));
        }
    }

    let can_build_cast = if requires_nested_struct_cast(&expr_type, cast_type) {
        // Allow casts involving structs (including nested inside Lists, Dictionaries,
        // etc.) that pass name-based compatibility validation. This validation is
        // applied at planning time (now) to fail fast, rather than deferring errors
        // to execution time. The name-based casting logic will be executed at runtime
        // via ColumnarValue::cast_to.
        can_cast_named_struct_types(&expr_type, cast_type)
    } else {
        can_cast_types(&expr_type, cast_type)
    };

    if !can_build_cast {
        return not_impl_err!("Unsupported CAST from {expr_type} to {cast_type}");
    }

    // For type-only casts, use CastExpr::new which preserves source metadata/nullability.
    // For explicit target fields, use new_with_target_field which applies the target's
    // extension metadata and nullability.
    if is_type_only {
        Ok(Arc::new(CastExpr::new(
            expr,
            cast_type.clone(),
            cast_options,
        )))
    } else {
        Ok(Arc::new(CastExpr::new_with_target_field(
            expr,
            Arc::clone(target_field),
            cast_options,
        )))
    }
}

/// Return a PhysicalExpression representing `expr` casted to
/// `cast_type`, if any casting is needed.
///
/// Note that such casts may lose type information
pub fn cast(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    cast_with_options(expr, input_schema, cast_type, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::column::col;

    use arrow::{
        array::{
            Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array,
            Int16Array, Int32Array, Int64Array, StringArray, StructArray,
            Time64NanosecondArray, TimestampNanosecondArray, UInt32Array,
        },
        datatypes::*,
    };
    use datafusion_common::ScalarValue;
    use datafusion_common::cast::{
        as_boolean_array, as_int64_array, as_string_array, as_struct_array,
        as_uint8_array,
    };
    use datafusion_physical_expr_common::physical_expr::fmt_sql;
    use insta::assert_snapshot;
    use std::collections::HashMap;

    fn make_struct_array(fields: Fields, arrays: Vec<ArrayRef>) -> StructArray {
        StructArray::new(fields, arrays, None)
    }

    fn cast_struct_array(
        column: &str,
        input_field: Field,
        target_field: Field,
        input_array: StructArray,
    ) -> Result<StructArray> {
        let schema = Arc::new(Schema::new(vec![input_field]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(input_array) as ArrayRef],
        )?;
        let expr = CastExpr::new_with_target_field(
            col(column, schema.as_ref())?,
            Arc::new(target_field),
            None,
        );

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        Ok(as_struct_array(result.as_ref())?.clone())
    }

    // runs an end-to-end test of physical type cast
    // 1. construct a record batch with a column "a" of type A
    // 2. construct a physical expression of CAST(a AS B)
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type B
    // 5. verify that the resulting values are downcastable and correct
    macro_rules! generic_decimal_to_other_test_cast {
        ($DECIMAL_ARRAY:ident, $A_TYPE:expr, $TYPEARRAY:ident, $TYPE:expr, $VEC:expr,$CAST_OPTIONS:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $A_TYPE, true)]);
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new($DECIMAL_ARRAY)],
            )?;
            // verify that we can construct the expression
            let expression =
                cast_with_options(col("a", &schema)?, &schema, $TYPE, $CAST_OPTIONS)?;

            // verify that its display is correct
            assert_eq!(format!("CAST(a@0 AS {})", $TYPE), format!("{}", expression));

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $TYPE);

            // compute
            let result = expression
                .evaluate(&batch)?
                .into_array(batch.num_rows())
                .expect("Failed to convert to array");

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $TYPE);

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$TYPEARRAY>()
                .expect("failed to downcast");

            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                match x {
                    Some(x) => assert_eq!(result.value(i), *x),
                    None => assert!(result.is_null(i)),
                }
            }
        }};
    }

    // runs an end-to-end test of physical type cast
    // 1. construct a record batch with a column "a" of type A
    // 2. construct a physical expression of CAST(a AS B)
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type B
    // 5. verify that the resulting values are downcastable and correct
    macro_rules! generic_test_cast {
        ($A_ARRAY:ident, $A_TYPE:expr, $A_VEC:expr, $TYPEARRAY:ident, $TYPE:expr, $VEC:expr, $CAST_OPTIONS:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $A_TYPE, true)]);
            let a_vec_len = $A_VEC.len();
            let a = $A_ARRAY::from($A_VEC);
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

            // verify that we can construct the expression
            let expression =
                cast_with_options(col("a", &schema)?, &schema, $TYPE, $CAST_OPTIONS)?;

            // verify that its display is correct
            assert_eq!(format!("CAST(a@0 AS {})", $TYPE), format!("{}", expression));

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $TYPE);

            // compute
            let result = expression
                .evaluate(&batch)?
                .into_array(batch.num_rows())
                .expect("Failed to convert to array");

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $TYPE);

            // verify that the len is correct
            assert_eq!(result.len(), a_vec_len);

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$TYPEARRAY>()
                .expect("failed to downcast");

            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                match x {
                    Some(x) => assert_eq!(result.value(i), *x),
                    None => assert!(result.is_null(i)),
                }
            }
        }};
    }

    #[test]
    fn test_cast_decimal_to_decimal() -> Result<()> {
        let array = vec![
            Some(1234),
            Some(2222),
            Some(3),
            Some(4000),
            Some(5000),
            None,
        ];

        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;

        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 3),
            Decimal128Array,
            Decimal128(20, 6),
            [
                Some(1_234_000),
                Some(2_222_000),
                Some(3_000),
                Some(4_000_000),
                Some(5_000_000),
                None
            ],
            None
        );

        let decimal_array = array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;

        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 3),
            Decimal128Array,
            Decimal128(10, 2),
            [Some(123), Some(222), Some(0), Some(400), Some(500), None],
            None
        );

        Ok(())
    }

    #[test]
    fn test_cast_decimal_to_decimal_overflow() -> Result<()> {
        let array = vec![Some(123456789)];

        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;

        let schema = Schema::new(vec![Field::new("a", Decimal128(10, 3), false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(decimal_array)],
        )?;
        let expression =
            cast_with_options(col("a", &schema)?, &schema, Decimal128(6, 2), None)?;
        let e = expression.evaluate(&batch).unwrap_err().strip_backtrace(); // panics on OK
        assert_snapshot!(e, @r"
        Failed to cast field 'a' from Decimal128(10, 3) to Decimal128(6, 2)
        caused by
        Arrow error: Invalid argument error: 123456.79 is too large to store in a Decimal128 of precision 6. Max is 9999.99
        ");
        // safe cast should return null
        let expression_safe = cast_with_options(
            col("a", &schema)?,
            &schema,
            Decimal128(6, 2),
            Some(DEFAULT_SAFE_CAST_OPTIONS),
        )?;
        let result_safe = expression_safe
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("failed to convert to array");

        assert!(result_safe.is_null(0));

        Ok(())
    }

    #[test]
    fn test_cast_decimal_to_numeric() -> Result<()> {
        let array = vec![Some(1), Some(2), Some(3), Some(4), Some(5), None];
        // decimal to i8
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int8Array,
            Int8,
            [
                Some(1_i8),
                Some(2_i8),
                Some(3_i8),
                Some(4_i8),
                Some(5_i8),
                None
            ],
            None
        );

        // decimal to i16
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int16Array,
            Int16,
            [
                Some(1_i16),
                Some(2_i16),
                Some(3_i16),
                Some(4_i16),
                Some(5_i16),
                None
            ],
            None
        );

        // decimal to i32
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int32Array,
            Int32,
            [
                Some(1_i32),
                Some(2_i32),
                Some(3_i32),
                Some(4_i32),
                Some(5_i32),
                None
            ],
            None
        );

        // decimal to i64
        let decimal_array = array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 0),
            Int64Array,
            Int64,
            [
                Some(1_i64),
                Some(2_i64),
                Some(3_i64),
                Some(4_i64),
                Some(5_i64),
                None
            ],
            None
        );

        // decimal to float32
        let array = vec![
            Some(1234),
            Some(2222),
            Some(3),
            Some(4000),
            Some(5000),
            None,
        ];
        let decimal_array = array
            .clone()
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(10, 3)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(10, 3),
            Float32Array,
            Float32,
            [
                Some(1.234_f32),
                Some(2.222_f32),
                Some(0.003_f32),
                Some(4.0_f32),
                Some(5.0_f32),
                None
            ],
            None
        );

        // decimal to float64
        let decimal_array = array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(20, 6)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            Decimal128(20, 6),
            Float64Array,
            Float64,
            [
                Some(0.001234_f64),
                Some(0.002222_f64),
                Some(0.000003_f64),
                Some(0.004_f64),
                Some(0.005_f64),
                None
            ],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_numeric_to_decimal() -> Result<()> {
        // int8
        generic_test_cast!(
            Int8Array,
            Int8,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(3, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int16
        generic_test_cast!(
            Int16Array,
            Int16,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(5, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int32
        generic_test_cast!(
            Int32Array,
            Int32,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(10, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int64
        generic_test_cast!(
            Int64Array,
            Int64,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(20, 0),
            [Some(1), Some(2), Some(3), Some(4), Some(5)],
            None
        );

        // int64 to different scale
        generic_test_cast!(
            Int64Array,
            Int64,
            vec![1, 2, 3, 4, 5],
            Decimal128Array,
            Decimal128(20, 2),
            [Some(100), Some(200), Some(300), Some(400), Some(500)],
            None
        );

        // float32
        generic_test_cast!(
            Float32Array,
            Float32,
            vec![1.5, 2.5, 3.0, 1.123_456_8, 5.50],
            Decimal128Array,
            Decimal128(10, 2),
            [Some(150), Some(250), Some(300), Some(112), Some(550)],
            None
        );

        // float64
        generic_test_cast!(
            Float64Array,
            Float64,
            vec![1.5, 2.5, 3.0, 1.123_456_8, 5.50],
            Decimal128Array,
            Decimal128(20, 4),
            [
                Some(15000),
                Some(25000),
                Some(30000),
                Some(11235),
                Some(55000)
            ],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_i32_u32() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            Int32,
            vec![1, 2, 3, 4, 5],
            UInt32Array,
            UInt32,
            [
                Some(1_u32),
                Some(2_u32),
                Some(3_u32),
                Some(4_u32),
                Some(5_u32)
            ],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_i32_utf8() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            Int32,
            vec![1, 2, 3, 4, 5],
            StringArray,
            Utf8,
            [Some("1"), Some("2"), Some("3"), Some("4"), Some("5")],
            None
        );
        Ok(())
    }

    #[test]
    fn test_cast_i64_t64() -> Result<()> {
        let original = vec![1, 2, 3, 4, 5];
        let expected: Vec<Option<i64>> = original
            .iter()
            .map(|i| Some(Time64NanosecondArray::from(vec![*i]).value(0)))
            .collect();
        generic_test_cast!(
            Int64Array,
            Int64,
            original,
            TimestampNanosecondArray,
            Timestamp(TimeUnit::Nanosecond, None),
            expected,
            None
        );
        Ok(())
    }

    // Tests for timestamp timezone casting have been moved to timestamps.slt
    // See the "Casting between timestamp with and without timezone" section

    #[test]
    fn invalid_cast() {
        // Ensure a useful error happens at plan time if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", Int32, false)]);

        let result = cast(
            col("a", &schema).unwrap(),
            &schema,
            Interval(IntervalUnit::MonthDayNano),
        );
        result.expect_err("expected Invalid CAST");
    }

    #[test]
    fn invalid_cast_with_options_error() -> Result<()> {
        // Ensure a useful error happens at runtime if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", Utf8, false)]);
        let a = StringArray::from(vec!["9.1"]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        let expression = cast_with_options(col("a", &schema)?, &schema, Int32, None)?;
        let result = expression.evaluate(&batch);

        let error = result.expect_err("expected error").strip_backtrace();
        assert_eq!(
            error,
            "Failed to cast field 'a' from Utf8 to Int32\n\
             caused by\n\
             Arrow error: Cast error: Cannot cast string '9.1' to value of Int32 type"
        );
        Ok(())
    }

    #[test]
    fn invalid_cast_with_empty_field_name_uses_expression() {
        let schema = Schema::new(vec![Field::new("", Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec!["9.1"]))],
        )
        .expect("valid record batch");
        let expression = cast_with_options(
            col("", &schema).expect("valid column"),
            &schema,
            Int32,
            None,
        )
        .expect("valid cast expression");

        let error = expression
            .evaluate(&batch)
            .expect_err("expected error")
            .strip_backtrace();
        assert_eq!(
            error,
            "Failed to cast expression '@0' from Utf8 to Int32\n\
             caused by\n\
             Arrow error: Cast error: Cannot cast string '9.1' to value of Int32 type"
        );
    }

    #[test]
    fn field_aware_cast_preserves_target_field_semantics() -> Result<()> {
        // Target field metadata should be preserved exactly (no merging with source).
        let metadata = HashMap::from([("target_meta".to_string(), "1".to_string())]);

        for (child_nullable, target_nullable) in [(true, false), (false, true)] {
            let schema = Schema::new(vec![Field::new("a", Int32, child_nullable)]);
            let target_field = Arc::new(
                Field::new("cast_target", Int64, target_nullable)
                    .with_metadata(metadata.clone()),
            );
            let expr = CastExpr::new_with_target_field(
                col("a", &schema)?,
                Arc::clone(&target_field),
                None,
            );

            let field = expr.return_field(&schema)?;
            // Field name comes from source
            assert_eq!(field.name(), "a");
            assert_eq!(field.data_type(), &Int64);
            // Nullability comes from target
            assert_eq!(field.is_nullable(), target_nullable);
            // Target metadata should be preserved exactly
            assert_eq!(
                field.metadata().get("target_meta"),
                Some(&"1".to_string()),
                "Target metadata should be preserved exactly"
            );
            assert_eq!(expr.nullable(&schema)?, child_nullable || target_nullable);
        }

        Ok(())
    }

    #[test]
    fn target_field_accessor_returns_the_constructed_field() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", Int32, true)]);
        let metadata = HashMap::from([("target_meta".to_string(), "1".to_string())]);
        let target_field =
            Arc::new(Field::new("cast_target", Int64, false).with_metadata(metadata));

        let expr = CastExpr::new_with_target_field(
            col("a", &schema)?,
            Arc::clone(&target_field),
            None,
        );

        // The field is returned verbatim, including its name.
        assert_eq!(expr.target_field(), &target_field);
        assert_eq!(expr.cast_type(), &Int64);
        assert_eq!(expr.target_metadata(), Some(target_field.metadata()));
        assert_eq!(expr.target_nullable(), Some(false));
        assert!(expr.has_explicit_metadata());
        assert!(expr.has_explicit_nullability());

        // A type-only cast reports no explicit target.
        let type_only = CastExpr::new(col("a", &schema)?, Int64, None);
        assert_eq!(type_only.cast_type(), &Int64);
        assert_eq!(type_only.target_metadata(), None);
        assert_eq!(type_only.target_nullable(), None);
        assert!(!type_only.has_explicit_metadata());
        assert!(!type_only.has_explicit_nullability());

        Ok(())
    }

    #[test]
    fn type_only_cast_preserves_legacy_field_name_and_nullability() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", Int32, false)]);
        let expr = CastExpr::new(col("a", &schema)?, Int64, None);

        let field = expr.return_field(&schema)?;

        assert_eq!(field.name(), "a");
        assert_eq!(field.data_type(), &Int64);
        assert!(!field.is_nullable());
        assert!(!expr.nullable(&schema)?);

        Ok(())
    }

    #[test]
    fn struct_cast_validation_uses_nested_target_fields() -> Result<()> {
        let source_type = Struct(Fields::from(vec![
            Arc::new(Field::new("x", Int32, true)),
            Arc::new(Field::new("y", Utf8, true)),
        ]));
        let schema = Schema::new(vec![Field::new("a", source_type.clone(), true)]);

        let valid_target = Struct(Fields::from(vec![
            Arc::new(Field::new("y", Utf8, true)),
            Arc::new(Field::new("x", Int64, true)),
        ]));
        cast_with_options(col("a", &schema)?, &schema, valid_target, None)?;

        let invalid_target = Struct(Fields::from(vec![
            Arc::new(Field::new("y", Utf8, true)),
            Arc::new(Field::new("missing", Int64, false)),
        ]));
        let err = cast_with_options(col("a", &schema)?, &schema, invalid_target, None)
            .expect_err("missing required struct field should fail");

        assert!(err.to_string().contains("Unsupported CAST"));

        Ok(())
    }

    #[test]
    fn field_aware_cast_struct_array_missing_child() -> Result<()> {
        let source_a = Field::new("a", Int32, true);
        let source_b = Field::new("b", Utf8, true);
        let target_field = Field::new(
            "s",
            Struct(
                vec![
                    Arc::new(Field::new("a", Int64, true)),
                    Arc::new(Field::new("c", Utf8, true)),
                ]
                .into(),
            ),
            true,
        );

        let struct_array = cast_struct_array(
            "s",
            Field::new(
                "s",
                Struct(
                    vec![Arc::new(source_a.clone()), Arc::new(source_b.clone())].into(),
                ),
                true,
            ),
            target_field,
            make_struct_array(
                vec![Arc::new(source_a), Arc::new(source_b)].into(),
                vec![
                    Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
                    Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]))
                        as ArrayRef,
                ],
            ),
        )?;
        let cast_a = as_int64_array(struct_array.column_by_name("a").unwrap().as_ref())?;
        assert_eq!(cast_a.value(0), 1);
        assert!(cast_a.is_null(1));

        let cast_c = as_string_array(struct_array.column_by_name("c").unwrap().as_ref())?;
        assert!(cast_c.is_null(0));
        assert!(cast_c.is_null(1));
        Ok(())
    }

    #[test]
    fn field_aware_cast_nested_struct_array() -> Result<()> {
        let inner_source = Field::new(
            "inner",
            Struct(vec![Arc::new(Field::new("x", Int32, true))].into()),
            true,
        );
        let inner_target = Field::new(
            "inner",
            Struct(
                vec![
                    Arc::new(Field::new("x", Int64, true)),
                    Arc::new(Field::new("y", Boolean, true)),
                ]
                .into(),
            ),
            true,
        );
        let target_field =
            Field::new("root", Struct(vec![Arc::new(inner_target)].into()), true);

        let inner_struct = make_struct_array(
            vec![Arc::new(Field::new("x", Int32, true))].into(),
            vec![Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef],
        );
        let outer_struct = make_struct_array(
            vec![Arc::new(inner_source.clone())].into(),
            vec![Arc::new(inner_struct) as ArrayRef],
        );
        let struct_array = cast_struct_array(
            "root",
            Field::new("root", Struct(vec![Arc::new(inner_source)].into()), true),
            target_field,
            outer_struct,
        )?;
        let inner =
            as_struct_array(struct_array.column_by_name("inner").unwrap().as_ref())?;
        let x = as_int64_array(inner.column_by_name("x").unwrap().as_ref())?;
        assert_eq!(x.value(0), 7);
        assert!(x.is_null(1));
        let y = as_boolean_array(inner.column_by_name("y").unwrap().as_ref())?;
        assert!(y.is_null(0));
        assert!(y.is_null(1));
        Ok(())
    }

    #[test]
    fn field_aware_cast_struct_scalar() -> Result<()> {
        let source_field = Field::new("a", Int32, true);
        let target_field = Field::new(
            "s",
            Struct(vec![Arc::new(Field::new("a", UInt8, true))].into()),
            true,
        );

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            Struct(vec![Arc::new(source_field.clone())].into()),
            true,
        )]));
        let scalar_struct = make_struct_array(
            vec![Arc::new(source_field)].into(),
            vec![Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef],
        );
        let literal = Arc::new(crate::expressions::Literal::new(ScalarValue::Struct(
            Arc::new(scalar_struct),
        )));
        let target_field = Arc::new(target_field);
        let expr = CastExpr::new_with_target_field(literal, target_field, None);

        let batch = RecordBatch::new_empty(schema);
        let result = expr.evaluate(&batch)?;
        let ColumnarValue::Scalar(ScalarValue::Struct(array)) = result else {
            panic!("expected struct scalar");
        };
        let casted = as_uint8_array(array.column_by_name("a").unwrap().as_ref())?;
        assert_eq!(casted.value(0), 9);
        Ok(())
    }

    #[test]
    #[ignore = "TODO: https://github.com/apache/datafusion/issues/5396"]
    fn test_cast_decimal() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", Int64, false)]);
        let a = Int64Array::from(vec![100]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        let expression =
            cast_with_options(col("a", &schema)?, &schema, Decimal128(38, 38), None)?;
        expression.evaluate(&batch)?;
        Ok(())
    }

    #[test]
    fn test_fmt_sql() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", Int32, true)]);

        // Test numeric casting
        let expr = cast(col("a", &schema)?, &schema, Int64)?;
        let display_string = expr.to_string();
        assert_eq!(display_string, "CAST(a@0 AS Int64)");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "CAST(a AS Int64)");

        // Test string casting
        let schema = Schema::new(vec![Field::new("b", Utf8, true)]);
        let expr = cast(col("b", &schema)?, &schema, Int32)?;
        let display_string = expr.to_string();
        assert_eq!(display_string, "CAST(b@0 AS Int32)");
        let sql_string = fmt_sql(expr.as_ref()).to_string();
        assert_eq!(sql_string, "CAST(b AS Int32)");

        Ok(())
    }

    #[test]
    fn type_only_cast_strips_extension_metadata() -> Result<()> {
        // When using type-only cast (new()), extension metadata from source should NOT propagate
        let source_meta = HashMap::from([
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "arrow.uuid".to_string(),
            ),
            ("custom_key".to_string(), "custom_value".to_string()),
        ]);
        let schema = Schema::new(vec![
            Field::new("a", FixedSizeBinary(16), false).with_metadata(source_meta),
        ]);

        let expr = CastExpr::new(col("a", &schema)?, Utf8, None);

        let field = expr.return_field(&schema)?;
        assert!(
            field.metadata().get(EXTENSION_TYPE_NAME_KEY).is_none(),
            "Type-only cast should strip extension type name from source"
        );
        assert_eq!(
            field.metadata().get("custom_key"),
            Some(&"custom_value".to_string()),
            "Type-only cast should preserve non-extension metadata"
        );

        Ok(())
    }

    #[test]
    fn field_aware_cast_uses_exact_target_metadata() -> Result<()> {
        // When using field-aware cast, target's metadata should be used exactly
        let source_meta = HashMap::from([
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "source.type".to_string(),
            ),
            ("source_key".to_string(), "source_value".to_string()),
        ]);
        let target_meta = HashMap::from([
            (
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "target.type".to_string(),
            ),
            (
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                "target_ext_meta".to_string(),
            ),
            ("target_key".to_string(), "target_value".to_string()),
        ]);
        let schema = Schema::new(vec![
            Field::new("a", FixedSizeBinary(16), false).with_metadata(source_meta),
        ]);

        let target_field =
            Arc::new(Field::new("b", Utf8, true).with_metadata(target_meta));
        let expr = CastExpr::new_with_target_field(
            col("a", &schema)?,
            Arc::clone(&target_field),
            None,
        );

        let field = expr.return_field(&schema)?;
        assert_eq!(
            field.metadata().get(EXTENSION_TYPE_NAME_KEY),
            Some(&"target.type".to_string()),
            "Field-aware cast should use target's extension type name"
        );
        assert_eq!(
            field.metadata().get(EXTENSION_TYPE_METADATA_KEY),
            Some(&"target_ext_meta".to_string()),
            "Field-aware cast should use target's extension type metadata"
        );
        assert!(
            field.metadata().get("source_key").is_none(),
            "Field-aware cast should NOT preserve source metadata"
        );
        assert_eq!(
            field.metadata().get("target_key"),
            Some(&"target_value".to_string()),
            "Field-aware cast should preserve target's non-extension metadata"
        );

        Ok(())
    }

    #[test]
    fn test_check_bigger_cast_precision_loss() {
        use DataType::*;

        // Exact conversions without precision loss
        assert!(CastExpr::check_bigger_cast(&Int16, &Int8));
        assert!(CastExpr::check_bigger_cast(&Int64, &Int32));
        assert!(CastExpr::check_bigger_cast(&Float32, &Int16));
        assert!(CastExpr::check_bigger_cast(&Float32, &UInt16));
        assert!(CastExpr::check_bigger_cast(&Float64, &Int32));
        assert!(CastExpr::check_bigger_cast(&Float64, &UInt32));
        assert!(CastExpr::check_bigger_cast(&LargeUtf8, &Utf8));

        // Precision-losing int-to-float conversions should return false
        assert!(!CastExpr::check_bigger_cast(&Float32, &Int32));
        assert!(!CastExpr::check_bigger_cast(&Float32, &UInt32));
        assert!(!CastExpr::check_bigger_cast(&Float64, &Int64));
        assert!(!CastExpr::check_bigger_cast(&Float64, &UInt64));

        // Signed <-> Unsigned conversions should return false (not order-preserving due to negative values)
        assert!(!CastExpr::check_bigger_cast(&UInt16, &Int8));
        assert!(!CastExpr::check_bigger_cast(&UInt32, &Int16));
        assert!(!CastExpr::check_bigger_cast(&Int16, &UInt8));
    }
}

/// Tests for the `try_to_proto` / `try_from_proto` hooks.
#[cfg(all(test, feature = "proto"))]
mod proto_tests {
    use super::*;
    use crate::expressions::{Column, col};
    use crate::proto_test_util::{
        StubDecoder, StubEncoder, UnreachableDecoder, column_node,
    };
    use arrow::datatypes::Field;
    use datafusion_common::DataFusionError;
    use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
    use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
    use datafusion_proto_models::datafusion_common::ArrowType;
    use datafusion_proto_models::protobuf::{
        PhysicalCastNode, PhysicalExprNode, physical_expr_node,
    };

    #[derive(Debug)]
    struct TestFormatterFactory;

    impl ArrayFormatterFactory for TestFormatterFactory {
        fn create_array_formatter<'formatter>(
            &self,
            _array: &'formatter dyn arrow::array::Array,
            _options: &FormatOptions<'formatter>,
            _field: Option<&'formatter Field>,
        ) -> std::result::Result<
            Option<arrow::util::display::ArrayFormatter<'formatter>>,
            arrow::error::ArrowError,
        > {
            Ok(None)
        }
    }

    static TEST_FORMATTER_FACTORY: TestFormatterFactory = TestFormatterFactory;

    /// A `CastExpr` over an `Int32` column, casting to `Int64`.
    fn proto_cast_fixture() -> CastExpr {
        let schema = Schema::new(vec![Field::new("a", Int32, false)]);
        CastExpr::new(col("a", &schema).unwrap(), Int64, None)
    }

    fn proto_int64_arrow_type() -> ArrowType {
        (&Int64).try_into().unwrap()
    }

    /// Build a legacy `CastExpr` proto node with the given child and target type.
    fn proto_cast_node(
        expr: Option<Box<PhysicalExprNode>>,
        arrow_type: Option<ArrowType>,
    ) -> PhysicalExprNode {
        PhysicalExprNode {
            expr_id: None,
            expr_type: Some(physical_expr_node::ExprType::Cast(Box::new(
                PhysicalCastNode {
                    expr,
                    arrow_type,
                    target_field: None,
                    cast_options: None,
                },
            ))),
        }
    }

    fn encode_cast(cast: &CastExpr) -> PhysicalCastNode {
        let encoder = StubEncoder::ok();
        let node = cast
            .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
            .unwrap()
            .expect("CastExpr should encode to Some(node)");
        match node.expr_type {
            Some(physical_expr_node::ExprType::Cast(cast)) => *cast,
            other => panic!("expected a Cast node, got {other:?}"),
        }
    }

    fn round_trip_cast(cast: &CastExpr, schema: &Schema) -> CastExpr {
        let encoder = StubEncoder::ok();
        let node = cast
            .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
            .unwrap()
            .expect("CastExpr should encode to Some(node)");
        let decoder = StubDecoder::ok();
        CastExpr::try_from_proto(&node, &PhysicalExprDecodeCtx::new(schema, &decoder))
            .unwrap()
            .downcast_ref::<CastExpr>()
            .expect("decoded expr should be a CastExpr")
            .clone()
    }

    fn non_default_cast_options() -> CastOptions<'static> {
        CastOptions {
            safe: true,
            format_options: FormatOptions::new()
                .with_display_error(false)
                .with_null("NULL")
                .with_date_format(Some("%d/%m/%Y"))
                .with_datetime_format(Some("%d/%m/%Y %H:%M:%S"))
                .with_timestamp_format(Some("%s"))
                .with_timestamp_tz_format(Some("%+"))
                .with_time_format(Some("%H-%M-%S"))
                .with_duration_format(DurationFormat::ISO8601)
                .with_types_info(true)
                .with_quoted_strings(true),
        }
    }

    #[test]
    fn try_to_proto_encodes_cast_expr() {
        let cast = proto_cast_fixture();
        let encoder = StubEncoder::ok();
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let node = cast
            .try_to_proto(&ctx)
            .unwrap()
            .expect("CastExpr should encode to Some(node)");

        assert!(node.expr_id.is_none());
        let cast_node = match node.expr_type {
            Some(physical_expr_node::ExprType::Cast(cast_node)) => *cast_node,
            other => panic!("expected a Cast node, got {other:?}"),
        };
        assert!(cast_node.expr.is_some());

        let arrow_type = cast_node
            .arrow_type
            .as_ref()
            .expect("cast type should be encoded");
        let data_type: DataType = arrow_type.try_into().unwrap();
        assert_eq!(data_type, Int64);
        assert!(cast_node.target_field.is_none());
        assert!(cast_node.cast_options.is_none());
    }

    #[test]
    fn cast_target_field_survives_proto_round_trip() {
        let schema = Schema::new(vec![Field::new("a", Int32, true)]);
        let target = Arc::new(Field::new("target", Int64, false).with_metadata(
            HashMap::from([("extension".to_string(), "value".to_string())]),
        ));
        let cast = CastExpr::new_with_target_field(
            col("a", &schema).unwrap(),
            Arc::clone(&target),
            None,
        );

        assert!(encode_cast(&cast).target_field.is_some());
        let decoded = round_trip_cast(&cast, &schema);

        assert_eq!(decoded.target_field(), &target);
        assert_eq!(decoded.target_metadata(), Some(target.metadata()));
        assert_eq!(decoded.target_nullable(), Some(false));
        assert!(!decoded.return_field(&schema).unwrap().is_nullable());
    }

    #[test]
    fn explicit_default_shaped_cast_target_is_encoded() {
        let schema = Schema::new(vec![Field::new("a", Int32, false).with_metadata(
            HashMap::from([("source".to_string(), "value".to_string())]),
        )]);
        let cast = CastExpr::new_with_target_field(
            col("a", &schema).unwrap(),
            Int64.into_nullable_field_ref(),
            None,
        );

        assert!(encode_cast(&cast).target_field.is_some());
        let decoded = round_trip_cast(&cast, &schema);
        assert_eq!(decoded.target_metadata(), Some(&HashMap::new()));
        assert_eq!(decoded.target_nullable(), Some(true));
        let output = decoded.return_field(&schema).unwrap();
        assert!(output.metadata().is_empty());
        assert!(output.is_nullable());
    }

    #[test]
    fn type_only_cast_target_is_omitted_and_remains_type_only() {
        let metadata = HashMap::from([("source".to_string(), "value".to_string())]);
        let schema = Schema::new(vec![
            Field::new("a", Int32, false).with_metadata(metadata.clone()),
        ]);
        let cast = CastExpr::new(col("a", &schema).unwrap(), Int64, None);

        assert!(encode_cast(&cast).target_field.is_none());
        let decoded = round_trip_cast(&cast, &schema);

        assert_eq!(decoded.target_metadata(), None);
        assert_eq!(decoded.target_nullable(), None);
        let output = decoded.return_field(&schema).unwrap();
        assert_eq!(output.metadata(), &metadata);
        assert!(!output.is_nullable());
    }

    #[test]
    fn cast_options_survive_proto_round_trip() {
        let schema = Schema::new(vec![Field::new("a", Int32, false)]);
        let options = non_default_cast_options();
        let cast =
            CastExpr::new(col("a", &schema).unwrap(), Int64, Some(options.clone()));

        assert!(encode_cast(&cast).cast_options.is_some());
        assert_eq!(round_trip_cast(&cast, &schema).cast_options(), options);
    }

    #[test]
    fn custom_formatter_factory_is_rejected() {
        let schema = Schema::new(vec![Field::new("a", Int32, false)]);
        let cast = CastExpr::new(
            col("a", &schema).unwrap(),
            Int64,
            Some(CastOptions {
                safe: false,
                format_options: DEFAULT_FORMAT_OPTIONS
                    .with_formatter_factory(Some(&TEST_FORMATTER_FACTORY)),
            }),
        );
        let encoder = StubEncoder::ok();

        let err = cast
            .try_to_proto(&PhysicalExprEncodeCtx::new(&encoder))
            .unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::NotImplemented(msg)
                if msg.contains("custom formatter_factory")
        ));
    }

    #[test]
    fn try_to_proto_propagates_child_encode_error() {
        let cast = proto_cast_fixture();
        let encoder = StubEncoder::failing_on(1);
        let ctx = PhysicalExprEncodeCtx::new(&encoder);

        let err = cast.try_to_proto(&ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg) if msg.contains("call 1")
        ));
    }

    #[test]
    fn try_from_proto_decodes_legacy_cast_expr() {
        let node = proto_cast_node(
            Some(Box::new(column_node("a"))),
            Some(proto_int64_arrow_type()),
        );
        let schema = Schema::empty();
        let decoder = StubDecoder::ok();
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let decoded = CastExpr::try_from_proto(&node, &ctx).unwrap();
        let cast = decoded
            .downcast_ref::<CastExpr>()
            .expect("decoded expr should be a CastExpr");

        assert_eq!(cast.cast_type(), &Int64);
        assert!(cast.expr().downcast_ref::<Column>().is_some());
        assert_eq!(cast.target_metadata(), None);
        assert_eq!(cast.target_nullable(), None);
        assert_eq!(cast.cast_options(), DEFAULT_CAST_OPTIONS);
    }

    #[test]
    fn try_from_proto_rejects_mismatched_target_field_type() {
        let mut node = proto_cast_node(
            Some(Box::new(column_node("a"))),
            Some(proto_int64_arrow_type()),
        );
        let Some(physical_expr_node::ExprType::Cast(cast)) = node.expr_type.as_mut()
        else {
            unreachable!()
        };
        cast.target_field =
            Some((&Field::new("target", Int32, true)).try_into().unwrap());
        let schema = Schema::empty();
        let decoder = StubDecoder::ok();

        let err = CastExpr::try_from_proto(
            &node,
            &PhysicalExprDecodeCtx::new(&schema, &decoder),
        )
        .unwrap_err();
        assert!(err.to_string().contains("target_field type"));
    }

    #[test]
    fn try_from_proto_rejects_non_cast_node() {
        let node = column_node("a");
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = CastExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg)
                if msg.contains("PhysicalExprNode is not a CastExpr")
        ));
    }

    #[test]
    fn try_from_proto_rejects_missing_expr() {
        let node = proto_cast_node(None, Some(proto_int64_arrow_type()));
        let schema = Schema::empty();
        let decoder = UnreachableDecoder;
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = CastExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg)
                if msg.contains("CastExpr is missing required field 'expr'")
        ));
    }

    #[test]
    fn try_from_proto_rejects_missing_arrow_type() {
        let node = proto_cast_node(Some(Box::new(column_node("a"))), None);
        let schema = Schema::empty();
        let decoder = StubDecoder::ok();
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = CastExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg)
                if msg.contains("CastExpr is missing required field 'arrow_type'")
        ));
    }

    #[test]
    fn try_from_proto_propagates_child_decode_error() {
        let node = proto_cast_node(
            Some(Box::new(column_node("a"))),
            Some(proto_int64_arrow_type()),
        );
        let schema = Schema::empty();
        let decoder = StubDecoder::failing_on(1);
        let ctx = PhysicalExprDecodeCtx::new(&schema, &decoder);

        let err = CastExpr::try_from_proto(&node, &ctx).unwrap_err();
        assert!(matches!(
            err,
            DataFusionError::Internal(msg) if msg.contains("call 1")
        ));
    }
}
