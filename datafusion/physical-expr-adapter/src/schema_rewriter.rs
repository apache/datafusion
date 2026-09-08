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

//! Physical expression schema rewriting utilities: [`PhysicalExprAdapter`],
//! [`PhysicalExprAdapterFactory`], default implementations,
//! and [`replace_columns_with_literals`].

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, FieldRef, Fields, SchemaRef};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_err,
    metadata::FieldMetadata,
    nested_struct::{requires_nested_struct_cast, validate_data_type_compatibility},
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::PhysicalExprSimplifier;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::{
    ScalarFunctionExpr,
    expressions::{self, CastExpr, Column},
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use itertools::Itertools;

/// Replace column references in the given physical expression with literal values.
///
/// Some use cases for this include:
/// - Partition column pruning: When scanning partitioned data, partition column references
///   can be replaced with their literal values for the specific partition being scanned.
/// - Constant folding: In some cases, columns that can be proven to be constant
///   from statistical analysis may be replaced with their literal values to optimize expression evaluation.
/// - Filling in non-null default values: in a custom [`PhysicalExprAdapter`] implementation,
///   column references can be replaced with default literal values instead of nulls.
///
/// # Arguments
/// - `expr`: The physical expression in which to replace column references.
/// - `replacements`: A mapping from column names to their corresponding literal `ScalarValue`s.
///   Accepts various HashMap types including `HashMap<&str, &ScalarValue>`,
///   `HashMap<String, ScalarValue>`, `HashMap<String, &ScalarValue>`, etc.
///
/// # Returns
/// - `Result<Arc<dyn PhysicalExpr>>`: The rewritten physical expression with columns replaced by literals.
pub fn replace_columns_with_literals<K, V>(
    expr: Arc<dyn PhysicalExpr>,
    replacements: &HashMap<K, V>,
) -> Result<Arc<dyn PhysicalExpr>>
where
    K: Borrow<str> + Eq + Hash,
    V: Borrow<ScalarValue>,
{
    expr.transform_down(|expr| {
        if let Some(column) = expr.downcast_ref::<Column>()
            && let Some(replacement_value) = replacements.get(column.name())
        {
            return Ok(Transformed::yes(expressions::lit(
                replacement_value.borrow().clone(),
            )));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

/// Trait for adapting [`PhysicalExpr`] expressions to match a target schema.
///
/// This is used in file scans to rewrite expressions so that they can be
/// evaluated against the physical schema of the file being scanned. It allows
/// for handling differences between logical and physical schemas, such as type
/// mismatches or missing columns common in [Schema evolution] scenarios.
///
/// [Schema evolution]: https://www.dremio.com/wiki/schema-evolution/
///
/// ## Default Implementations
///
/// The default implementation [`DefaultPhysicalExprAdapter`]  handles common
/// cases.
///
/// ## Custom Implementations
///
/// You can create a custom implementation of this trait to handle specific rewriting logic.
/// For example, to fill in missing columns with default values instead of nulls:
///
/// ```rust
/// use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
/// use arrow::datatypes::{Schema, Field, DataType, FieldRef, SchemaRef};
/// use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
/// use datafusion_common::{Result, ScalarValue, tree_node::{Transformed, TransformedResult, TreeNode}};
/// use datafusion_physical_expr::expressions::{self, Column};
/// use std::sync::Arc;
///
/// #[derive(Debug)]
/// pub struct CustomPhysicalExprAdapter {
///     logical_file_schema: SchemaRef,
///     physical_file_schema: SchemaRef,
/// }
///
/// impl PhysicalExprAdapter for CustomPhysicalExprAdapter {
///     fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
///         expr.transform(|expr| {
///             if let Some(column) = expr.downcast_ref::<Column>() {
///                 // Check if the column exists in the physical schema
///                 if self.physical_file_schema.index_of(column.name()).is_err() {
///                     // If the column is missing, fill it with a default value instead of null
///                     // The default value could be stored in the table schema's column metadata for example.
///                     let default_value = ScalarValue::Int32(Some(0));
///                     return Ok(Transformed::yes(expressions::lit(default_value)));
///                 }
///             }
///             // If the column exists, return it as is
///             Ok(Transformed::no(expr))
///         }).data()
///     }
/// }
///
/// #[derive(Debug)]
/// pub struct CustomPhysicalExprAdapterFactory;
///
/// impl PhysicalExprAdapterFactory for CustomPhysicalExprAdapterFactory {
///     fn create(
///         &self,
///         logical_file_schema: SchemaRef,
///         physical_file_schema: SchemaRef,
///     ) -> Result<Arc<dyn PhysicalExprAdapter>> {
///         Ok(Arc::new(CustomPhysicalExprAdapter {
///             logical_file_schema,
///             physical_file_schema,
///         }))
///     }
/// }
/// ```
pub trait PhysicalExprAdapter: Send + Sync + std::fmt::Debug {
    /// Rewrite a physical expression to match the target schema.
    ///
    /// This method should return a transformed expression that matches the target schema.
    ///
    /// Arguments:
    /// - `expr`: The physical expression to rewrite.
    /// - `logical_file_schema`: The logical schema of the table being queried, excluding any partition columns.
    /// - `physical_file_schema`: The physical schema of the file being scanned.
    /// - `partition_values`: Optional partition values to use for rewriting partition column references.
    ///   These are handled as if they were columns appended onto the logical file schema.
    ///
    /// Returns:
    /// - `Arc<dyn PhysicalExpr>`: The rewritten physical expression that can be evaluated against the physical schema.
    ///
    /// See Also:
    /// - [`replace_columns_with_literals`]: for replacing partition column references with their literal values.
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>;
}

/// Creates instances of [`PhysicalExprAdapter`] for given logical and physical schemas.
///
/// See [`DefaultPhysicalExprAdapterFactory`] for the default implementation.
pub trait PhysicalExprAdapterFactory: Send + Sync + std::fmt::Debug {
    /// Create a new instance of the physical expression adapter.
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>>;

    /// Whether replacing this factory with [`DefaultPhysicalExprAdapterFactory`]
    /// preserves execution behavior. Plan serializers may safely omit factories
    /// that return `true` because decoders use the default when none is configured.
    fn is_equivalent_to_default(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone)]
pub struct DefaultPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for DefaultPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(DefaultPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
        }))
    }

    fn is_equivalent_to_default(&self) -> bool {
        true
    }
}

/// Default implementation of [`PhysicalExprAdapter`] for rewriting physical
/// expressions to match different schemas.
///
/// ## Overview
///
///  [`DefaultPhysicalExprAdapter`] rewrites physical expressions to match
///  different schemas, including:
///
/// - **Type casting**: When logical and physical schemas have different types, expressions are
///   automatically wrapped with cast operations. For example, `lit(ScalarValue::Int32(123)) = int64_column`
///   gets rewritten to `lit(ScalarValue::Int32(123)) = cast(int64_column, 'Int32')`.
///   Note that this does not attempt to simplify such expressions - that is done by shared simplifiers.
///
/// - **Missing columns**: When a column exists in the logical schema but not in the physical schema,
///   references to it are replaced with null literals.
///
/// - **Struct field access**: Expressions like `struct_column.field_that_is_missing_in_schema` are
///   rewritten to `null` when the field doesn't exist in the physical schema.
///
/// - **Default column values**: Partition column references can be replaced with their literal values
///   when scanning specific partitions. See [`replace_columns_with_literals`] for more details.
///
/// # Example
///
/// ```rust
/// # use datafusion_physical_expr_adapter::{DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory};
/// # use arrow::datatypes::Schema;
/// # use std::sync::Arc;
/// #
/// # fn example(
/// #     predicate: std::sync::Arc<dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr>,
/// #     physical_file_schema: &Schema,
/// #     logical_file_schema: &Schema,
/// # ) -> datafusion_common::Result<()> {
/// let factory = DefaultPhysicalExprAdapterFactory;
/// let adapter =
///     factory.create(Arc::new(logical_file_schema.clone()), Arc::new(physical_file_schema.clone()))?;
/// let adapted_predicate = adapter.rewrite(predicate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DefaultPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl DefaultPhysicalExprAdapter {
    /// Create a new instance of the default physical expression adapter.
    ///
    /// This adapter rewrites expressions to match the physical schema of the file being scanned,
    /// handling type mismatches and missing columns by filling them with default values.
    pub fn new(logical_file_schema: SchemaRef, physical_file_schema: SchemaRef) -> Self {
        Self {
            logical_file_schema,
            physical_file_schema,
        }
    }
}

impl PhysicalExprAdapter for DefaultPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let mut rewriter = DefaultPhysicalExprAdapterRewriter {
            logical_file_schema: Arc::clone(&self.logical_file_schema),
            physical_file_schema: Arc::clone(&self.physical_file_schema),
            generated_struct_casts: HashMap::new(),
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }
}

struct DefaultPhysicalExprAdapterRewriter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    // A fresh map is created for each `rewrite()` call. Tracking relies on
    // bottom-up `transform` traversal: a generated child cast is recorded before
    // its parent `get_field` sees the same Arc allocation. Owned Arc clones keep
    // recorded allocations alive even after removal from the tree, preventing
    // pointer-address reuse. Keeping provenance here avoids adding markers to
    // expression types or threading it through rewrite results.
    generated_struct_casts: HashMap<*const (), Arc<dyn PhysicalExpr>>,
}

/// Outcome of walking a `get_field` key path through nested struct fields.
enum FieldPathResolution<'a> {
    /// The leaf field the path points at.
    Found(&'a FieldRef),
    /// Some key along the path does not exist, so the access reads as null.
    Missing,
    /// An intermediate field is not a struct, so the path cannot be resolved
    /// statically.
    NotAStruct,
}

/// Follow a `get_field` key path (`['a', 'b']` for `s['a']['b']`) through
/// nested struct fields.
///
/// The first key is taken separately from the rest so that the "at least one
/// key" invariant is carried by the signature: there is no empty path to
/// resolve.
fn resolve_field_path<'a>(
    fields: &'a Fields,
    field_name: &str,
    rest: &[&str],
) -> FieldPathResolution<'a> {
    let Some(field) = fields.iter().find(|f| f.name() == field_name) else {
        return FieldPathResolution::Missing;
    };
    let Some((next_field_name, rest)) = rest.split_first() else {
        return FieldPathResolution::Found(field);
    };
    match field.data_type() {
        DataType::Struct(nested_fields) => {
            resolve_field_path(nested_fields, next_field_name, rest)
        }
        _ => FieldPathResolution::NotAStruct,
    }
}

/// Whether a type or any nested value type matches the predicate.
fn contains_type(data_type: &DataType, predicate: &impl Fn(&DataType) -> bool) -> bool {
    if predicate(data_type) {
        return true;
    }
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::RunEndEncoded(_, field) => {
            contains_type(field.data_type(), predicate)
        }
        DataType::Map(entries, _) => {
            // The entries Struct is a layout wrapper, not a Struct-valued child.
            let DataType::Struct(fields) = entries.data_type() else {
                return false;
            };
            fields
                .iter()
                .any(|field| contains_type(field.data_type(), predicate))
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_type(field.data_type(), predicate)),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_type(field.data_type(), predicate)),
        DataType::Dictionary(_, values) => contains_type(values, predicate),
        _ => false,
    }
}

/// Retain only the selected field path in a cast target, preserving its Struct
/// ancestors' metadata and nullability. This excludes unselected sibling
/// conversions while keeping the all-null Struct shortcut for decimal casts.
fn retain_field_path(field: &FieldRef, path: &[&str]) -> Option<FieldRef> {
    let Some((name, rest)) = path.split_first() else {
        return Some(Arc::clone(field));
    };
    let DataType::Struct(fields) = field.data_type() else {
        return None;
    };
    let child = fields.iter().find(|child| child.name() == *name)?;
    let child = retain_field_path(child, rest)?;
    Some(Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(vec![child].into())),
    ))
}

impl DefaultPhysicalExprAdapterRewriter {
    fn rewrite_expr(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(transformed) = self.try_rewrite_struct_field_access(&expr)? {
            return Ok(Transformed::yes(transformed));
        }

        if let Some(transformed) = self.try_narrow_struct_cast(&expr)? {
            // A narrowed Struct cast may be accessed by another get_field.
            self.record_generated_struct_cast(&transformed);
            return Ok(Transformed::yes(transformed));
        }

        if let Some(column) = expr.downcast_ref::<Column>() {
            let transformed = self.rewrite_column(Arc::clone(&expr), column)?;
            self.record_generated_struct_cast(&transformed.data);
            return Ok(transformed);
        }

        Ok(Transformed::no(expr))
    }

    fn record_generated_struct_cast(&mut self, expr: &Arc<dyn PhysicalExpr>) {
        if expr
            .downcast_ref::<CastExpr>()
            .is_some_and(|cast| matches!(cast.cast_type(), DataType::Struct(_)))
        {
            self.generated_struct_casts
                .insert(Arc::as_ptr(expr).cast::<()>(), Arc::clone(expr));
        }
    }

    /// Rewrite `get_field(cast(s AS Struct<..>), 'f')` into
    /// `cast(get_field(s, 'f') AS <type of f>)`.
    ///
    /// Expressions are rewritten bottom-up, so by the time we reach a
    /// `get_field` node its struct argument has already been wrapped in a cast
    /// by [`Self::rewrite_column`] whenever the logical and physical struct
    /// types differ.
    ///
    /// Narrowing that cast is worthwhile for two reasons:
    ///
    /// 1. Reading one field should not cost a whole struct. The wide form
    ///    casts every field of the column — including ones the query never
    ///    reads — to produce a value that is immediately discarded except for
    ///    one field.
    /// 2. It keeps the column visible. Consumers throughout the codebase
    ///    pattern match on `get_field(column, 'f')` to recognise a struct
    ///    field access; a cast between the `get_field` and its column defeats
    ///    that match, and each such consumer then falls back to whatever it
    ///    does for an unrecognised expression.
    ///
    /// The Parquet scan is one such consumer, and the reason this is a
    /// correctness fix rather than only an optimisation: it decides at
    /// planning time, against the table schema, that a struct-field predicate
    /// can be evaluated as a row filter, and reports the predicate as fully
    /// handled. See <https://github.com/apache/datafusion/issues/24109>.
    ///
    /// Fixing it here rather than teaching that one consumer to see through
    /// casts is deliberate: the adapter is where the obscuring cast is
    /// introduced, so every consumer benefits, and no consumer has to loosen
    /// its pattern to accept arbitrary casts between a `get_field` and its
    /// column.
    ///
    /// `get_field` also has a flattened multi-key form: `s['a']['b']` is
    /// simplified to `get_field(s, 'a', 'b')`, so the whole field path is
    /// resolved here rather than just the first key.
    ///
    /// Only struct casts introduced by this adapter are narrowed. Explicit
    /// casts must still evaluate sibling conversions, which may fail.
    /// `get_field` on a Map column performs a
    /// runtime key lookup rather than a schema-level field access, so the map
    /// value must keep its cast.
    fn try_narrow_struct_cast(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let Some(get_field_expr) =
            ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
        else {
            return Ok(None);
        };
        let Some((source_expr, field_name_exprs)) = get_field_expr.args().split_first()
        else {
            return Ok(None);
        };
        let Some(cast) = source_expr.downcast_ref::<CastExpr>() else {
            return Ok(None);
        };
        if !self
            .generated_struct_casts
            .contains_key(&Arc::as_ptr(source_expr).cast::<()>())
        {
            return Ok(None);
        }

        // Every key has to be a string literal, otherwise the leaf field
        // cannot be resolved statically.
        let mut field_path = Vec::with_capacity(field_name_exprs.len());
        for field_name_expr in field_name_exprs {
            let Some(field_name) = field_name_expr
                .downcast_ref::<Literal>()
                .and_then(|lit| lit.value().try_as_str().flatten())
            else {
                return Ok(None);
            };
            field_path.push(field_name);
        }
        // A `get_field` with no keys is not a field access we can narrow.
        let Some((first_key, rest_keys)) = field_path.split_first() else {
            return Ok(None);
        };

        let DataType::Struct(logical_struct_fields) = cast.target_field().data_type()
        else {
            return Ok(None);
        };
        let FieldPathResolution::Found(logical_struct_field) =
            resolve_field_path(logical_struct_fields, first_key, rest_keys)
        else {
            return Ok(None);
        };

        let inner = cast.expr();
        let DataType::Struct(physical_struct_fields) =
            inner.data_type(&self.physical_file_schema)?
        else {
            return Ok(None);
        };
        let physical_struct_field =
            match resolve_field_path(&physical_struct_fields, first_key, rest_keys) {
                FieldPathResolution::Found(field) => field,
                FieldPathResolution::Missing => {
                    // The file does not have this field at all, so reading it
                    // yields null. Note that the cast would have produced the
                    // same value: struct casts fill missing target fields with
                    // nulls.
                    let null_value =
                        ScalarValue::Null.cast_to(logical_struct_field.data_type())?;
                    return Ok(Some(Arc::new(Literal::new_with_metadata(
                        null_value,
                        Some(FieldMetadata::from(logical_struct_field.as_ref())),
                    ))));
                }
                FieldPathResolution::NotAStruct => return Ok(None),
            };

        // Decimal conversions, including those inside containers, can fail
        // during setup even for all-null inputs, while a Struct cast skips its
        // children when the parent is all null.
        // Keep the Struct ancestors for that shortcut, but exclude unselected
        // siblings whose conversions may fail. Same-type metadata casts remain
        // safe to narrow to a scalar cast.
        // A Struct-to-Struct leaf cast keeps its own shortcut and must remain
        // narrowable by a parent get_field.
        // Container casts involving Struct values must keep their existing
        // dispatch: Arrow can unwrap a container into a Struct where cast_column
        // cannot. That leaves one shape uncovered: unwrapping hands the Struct
        // to Arrow's own cast, which has no all-null shortcut, so a decimal
        // below it can still fail on an all-null input (for example
        // `Dictionary(Int8, Struct<b: Utf8>)` to `Struct<b: Decimal128(10, -1)>`).
        // The Parquet reader does not produce dictionary-encoded Struct columns,
        // so this is not reachable through a Parquet scan; closing it would mean
        // telling "Arrow must unwrap this" apart from "Arrow will convert a
        // decimal while unwrapping" rather than dropping the carve-out.
        let source_type = physical_struct_field.data_type();
        let target_type = logical_struct_field.data_type();
        let is_struct = |data_type: &DataType| matches!(data_type, DataType::Struct(_));
        if source_type != target_type
            && !matches!(
                (source_type, target_type),
                (DataType::Struct(_), DataType::Struct(_))
            )
            && (contains_type(source_type, &DataType::is_decimal)
                || contains_type(target_type, &DataType::is_decimal))
            && (source_type.is_decimal()
                || target_type.is_decimal()
                || requires_nested_struct_cast(source_type, target_type)
                || (!contains_type(source_type, &is_struct)
                    && !contains_type(target_type, &is_struct)))
        {
            let Some(target_field) = retain_field_path(cast.target_field(), &field_path)
            else {
                return Ok(None);
            };
            let mut args = get_field_expr.args().to_vec();
            args[0] = Arc::new(CastExpr::new_with_target_field(
                Arc::clone(inner),
                target_field,
                Some(cast.cast_options().clone()),
            ));
            return Arc::clone(expr).with_new_children(args).map(Some);
        }

        // Rebuild `get_field` over the uncast struct so its return field is
        // recomputed from the physical field type.
        let mut args = Vec::with_capacity(get_field_expr.args().len());
        args.push(Arc::clone(inner));
        args.extend(field_name_exprs.iter().map(Arc::clone));
        let extracted = Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(get_field_expr.fun().clone()),
            args,
            &self.physical_file_schema,
            Arc::new(get_field_expr.config_options().clone()),
        )?) as Arc<dyn PhysicalExpr>;

        // get_field inherits nullability from every parent along the path.
        // Its complete return field can differ even when the leaf fields match.
        let logical_return_field = expr.return_field(&self.logical_file_schema)?;
        if physical_struct_field == logical_struct_field
            && extracted.return_field(&self.physical_file_schema)? == logical_return_field
        {
            return Ok(Some(extracted));
        }
        Ok(Some(Arc::new(CastExpr::new_with_target_field(
            extracted,
            logical_return_field,
            Some(cast.cast_options().clone()),
        ))))
    }

    /// Attempt to rewrite struct field access expressions to return null if the field does not exist in the physical schema.
    /// Note that this does *not* handle nested struct fields, only top-level struct field access.
    /// See <https://github.com/apache/datafusion/issues/17114> for more details.
    fn try_rewrite_struct_field_access(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let Some(get_field_expr) =
            ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
        else {
            return Ok(None);
        };

        let Some(source_expr) = get_field_expr.args().first() else {
            return Ok(None);
        };

        let Some(field_name_expr) = get_field_expr.args().get(1) else {
            return Ok(None);
        };

        let Some(lit) = field_name_expr.downcast_ref::<Literal>() else {
            return Ok(None);
        };

        let Some(field_name) = lit.value().try_as_str().flatten() else {
            return Ok(None);
        };

        let Some(column) = source_expr.downcast_ref::<Column>() else {
            return Ok(None);
        };

        let Ok(physical_field) = self.physical_file_schema.field_with_name(column.name())
        else {
            return Ok(None);
        };

        let DataType::Struct(physical_struct_fields) = physical_field.data_type() else {
            return Ok(None);
        };

        if physical_struct_fields
            .iter()
            .any(|f| f.name() == field_name)
        {
            return Ok(None);
        }

        let Ok(logical_field) = self.logical_file_schema.field_with_name(column.name())
        else {
            return Ok(None);
        };

        let DataType::Struct(logical_struct_fields) = logical_field.data_type() else {
            return Ok(None);
        };

        let Some(logical_struct_field) = logical_struct_fields
            .iter()
            .find(|f| f.name() == field_name)
        else {
            return Ok(None);
        };

        let null_value = ScalarValue::Null.cast_to(logical_struct_field.data_type())?;
        Ok(Some(Arc::new(Literal::new_with_metadata(
            null_value,
            Some(FieldMetadata::from(logical_struct_field.as_ref())),
        ))))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Get the logical field for this column if it exists in the logical schema
        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(e) => {
                // This can be hit if a custom rewrite injected a reference to a column that doesn't exist in the logical schema.
                // For example, a pre-computed column that is kept only in the physical schema.
                // If the column exists in the physical schema, we can still use it.
                if let Ok(physical_field) =
                    self.physical_file_schema.field_with_name(column.name())
                {
                    // If the column exists in the physical schema, we can use it in place of the logical column.
                    // This is nice to users because if they do a rewrite that results in something like `physical_int32_col = 123u64`
                    // we'll at least handle the casts for them.
                    physical_field
                } else {
                    // A completely unknown column that doesn't exist in either schema!
                    // This should probably never be hit unless something upstream broke, but nonetheless it's better
                    // for us to return a handleable error than to panic / do something unexpected.
                    return Err(e.into());
                }
            }
        };

        let Some((resolved_column, physical_field)) =
            self.resolve_physical_column(column)?
        else {
            if !logical_field.is_nullable() {
                return exec_err!(
                    "Non-nullable column '{}' is missing from the physical schema",
                    column.name()
                );
            }
            // If the column is missing from the physical schema fill it in with nulls.
            // For a different behavior, provide a custom `PhysicalExprAdapter` implementation.
            let null_value = ScalarValue::Null.cast_to(logical_field.data_type())?;
            return Ok(Transformed::yes(Arc::new(Literal::new_with_metadata(
                null_value,
                Some(FieldMetadata::from(logical_field)),
            ))));
        };

        let fields_match = logical_field == physical_field.as_ref();
        if fields_match {
            if resolved_column.index() == column.index() {
                return Ok(Transformed::no(expr));
            }

            // If the fields match (including metadata/nullability), we can use the column as is
            return Ok(Transformed::yes(Arc::new(resolved_column)));
        }

        // We need a cast expression whenever the logical and physical fields differ,
        // whether that difference is only metadata/nullability or also data type.
        // TODO: add optimization to move the cast from the column to literal expressions in the case of `col = 123`
        // since that's much cheaper to evaluate.
        // See https://github.com/apache/datafusion/issues/15780#issuecomment-2824716928
        validate_data_type_compatibility(
            resolved_column.name(),
            physical_field.data_type(),
            logical_field.data_type(),
        )
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type): {e}",
                resolved_column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            ))
        })?;

        Ok(Transformed::yes(Arc::new(CastExpr::new_with_target_field(
            Arc::new(resolved_column),
            Arc::new(logical_field.clone()),
            None,
        ))))
    }

    /// Resolves a logical column to the corresponding physical column and field.
    fn resolve_physical_column(
        &self,
        column: &Column,
    ) -> Result<Option<(Column, FieldRef)>> {
        // The physical schema adaptation step intentionally resolves columns by **name first**
        // rather than trusting the incoming index. This mirrors what the old refactoring
        // did before `resolve_physical_column()` was extracted: the planner might hand us a
        // `Column` whose `index` field is stale (e.g. after projection/rename rewrites), so
        // resolving by name ensures we match the correct physical slot. Once we know the
        // proper index we rebuild the `Column` with `new_with_schema` so callers can rely
        // on `column.index()` later without having to re-query the schema.
        let Ok(physical_column_index) = self.physical_file_schema.index_of(column.name())
        else {
            return Ok(None);
        };

        let column = if column.index() == physical_column_index {
            column.clone()
        } else {
            Column::new_with_schema(column.name(), self.physical_file_schema.as_ref())?
        };

        let physical_field = Arc::new(
            self.physical_file_schema
                .field(physical_column_index)
                .clone(),
        );

        Ok(Some((column, physical_field)))
    }
}

/// Factory for creating [`BatchAdapter`] instances to adapt record batches
/// to a target schema.
///
/// This binds a target schema and allows creating adapters for different source schemas.
/// It handles:
/// - **Column reordering**: Columns are reordered to match the target schema
/// - **Type casting**: Automatic type conversion (e.g., Int32 to Int64)
/// - **Missing columns**: Nullable columns missing from source are filled with nulls
/// - **Struct field adaptation**: Nested struct fields are recursively adapted
///
/// ## Examples
///
/// ```rust
/// use arrow::array::{Int32Array, Int64Array, StringArray, RecordBatch};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use datafusion_physical_expr_adapter::BatchAdapterFactory;
/// use std::sync::Arc;
///
/// // Target schema has different column order and types
/// let target_schema = Arc::new(Schema::new(vec![
///     Field::new("name", DataType::Utf8, true),
///     Field::new("id", DataType::Int64, false),    // Int64 in target
///     Field::new("score", DataType::Float64, true), // Missing from source
/// ]));
///
/// // Source schema has different column order and Int32 for id
/// let source_schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),    // Int32 in source
///     Field::new("name", DataType::Utf8, true),
///     // Note: 'score' column is missing from source
/// ]));
///
/// // Create factory with target schema
/// let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
///
/// // Create adapter for this specific source schema
/// let adapter = factory.make_adapter(&source_schema).unwrap();
///
/// // Create a source batch
/// let source_batch = RecordBatch::try_new(
///     source_schema,
///     vec![
///         Arc::new(Int32Array::from(vec![1, 2, 3])),
///         Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
///     ],
/// ).unwrap();
///
/// // Adapt the batch to match target schema
/// let adapted = adapter.adapt_batch(&source_batch).unwrap();
///
/// assert_eq!(adapted.num_columns(), 3);
/// assert_eq!(adapted.column(0).data_type(), &DataType::Utf8);   // name
/// assert_eq!(adapted.column(1).data_type(), &DataType::Int64);  // id (cast from Int32)
/// assert_eq!(adapted.column(2).data_type(), &DataType::Float64); // score (filled with nulls)
/// ```
#[derive(Debug)]
pub struct BatchAdapterFactory {
    target_schema: SchemaRef,
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
}

impl BatchAdapterFactory {
    /// Create a new [`BatchAdapterFactory`] with the given target schema.
    pub fn new(target_schema: SchemaRef) -> Self {
        let expr_adapter_factory = Arc::new(DefaultPhysicalExprAdapterFactory);
        Self {
            target_schema,
            expr_adapter_factory,
        }
    }

    /// Set a custom [`PhysicalExprAdapterFactory`] to use when adapting expressions.
    ///
    /// Use this to customize behavior when adapting batches, e.g. to fill in missing values
    /// with defaults instead of nulls.
    ///
    /// See [`PhysicalExprAdapter`] for more details.
    pub fn with_adapter_factory(
        self,
        factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Self {
        Self {
            expr_adapter_factory: factory,
            ..self
        }
    }

    /// Create a new [`BatchAdapter`] for the given source schema.
    ///
    /// Batches fed into this [`BatchAdapter`] *must* conform to the source schema,
    /// no validation is performed at runtime to minimize overheads.
    pub fn make_adapter(&self, source_schema: &SchemaRef) -> Result<BatchAdapter> {
        let expr_adapter = self
            .expr_adapter_factory
            .create(Arc::clone(&self.target_schema), Arc::clone(source_schema))?;

        let simplifier = PhysicalExprSimplifier::new(&self.target_schema);

        let projection = ProjectionExprs::from_indices(
            &(0..self.target_schema.fields().len()).collect_vec(),
            &self.target_schema,
        );

        let adapted = projection
            .try_map_exprs(|e| simplifier.simplify(expr_adapter.rewrite(e)?))?;
        let projector = adapted.make_projector(source_schema)?;

        Ok(BatchAdapter { projector })
    }
}

/// Adapter for transforming record batches to match a target schema.
///
/// Create instances via [`BatchAdapterFactory`].
///
/// ## Performance
///
/// The adapter pre-computes the projection expressions during creation,
/// so the [`adapt_batch`](BatchAdapter::adapt_batch) call is efficient and suitable
/// for use in hot paths like streaming file scans.
#[derive(Debug)]
pub struct BatchAdapter {
    projector: Projector,
}

impl BatchAdapter {
    /// Adapt the given record batch to match the target schema.
    ///
    /// The input batch *must* conform to the source schema used when
    /// creating this adapter.
    pub fn adapt_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.projector.project_batch(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, GenericListArray, Int32Array, Int64Array, RecordBatch,
        RecordBatchOptions, StringArray, StringViewArray, StructArray, record_batch,
    };
    use arrow::datatypes as arrow_schema;
    use arrow::datatypes::{Field, Fields, Schema};
    use datafusion_common::assert_contains;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{Column, Literal, col};

    fn assert_cast_expr(expr: &Arc<dyn PhysicalExpr>) -> &CastExpr {
        expr.downcast_ref::<CastExpr>().expect("Expected CastExpr")
    }

    fn assert_cast_input_column(cast_expr: &CastExpr, name: &str, index: usize) {
        let inner_col = cast_expr
            .expr()
            .downcast_ref::<Column>()
            .expect("Expected inner Column");
        assert_eq!(inner_col.name(), name);
        assert_eq!(inner_col.index(), index);
    }

    fn stale_index_cast_schemas() -> (SchemaRef, SchemaRef) {
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Binary, true),
            Field::new("a", DataType::Int32, false),
        ]));

        let logical_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Binary, true),
        ]));

        (logical_schema, physical_schema)
    }

    fn create_test_schema() -> (Schema, Schema) {
        let physical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);

        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false), // Different type
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true), // Missing from physical
        ]);

        (physical_schema, logical_schema)
    }

    #[test]
    fn test_rewrite_column_with_type_cast() {
        let (physical_schema, logical_schema) = create_test_schema();

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("a", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        // Should be wrapped in a cast expression
        assert!(result.downcast_ref::<CastExpr>().is_some());
    }

    #[test]
    fn test_rewrite_column_with_metadata_or_nullability_mismatch() -> Result<()> {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let logical_schema =
            Schema::new(vec![Field::new("a", DataType::Int64, false).with_metadata(
                HashMap::from([("logical_meta".to_string(), "1".to_string())]),
            )]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema.clone()))
            .unwrap();

        let result = adapter.rewrite(Arc::new(Column::new("a", 0)))?;

        // Ensure the expression preserves the logical field nullability/metadata.
        let return_field = result.return_field(physical_schema.as_ref())?;
        assert_eq!(return_field.data_type(), &DataType::Int64);
        assert!(!return_field.is_nullable());
        assert_eq!(
            return_field
                .metadata()
                .get("logical_meta")
                .map(String::as_str),
            Some("1")
        );

        Ok(())
    }

    #[test]
    fn test_rewrite_multi_column_expr_with_type_cast() {
        let (physical_schema, logical_schema) = create_test_schema();
        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();

        // Create a complex expression: (a + 5) OR (c > 0.0) that tests the recursive case of the rewriter
        let column_a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let column_c = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;
        let expr = expressions::BinaryExpr::new(
            Arc::clone(&column_a),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int64(Some(5)))),
        );
        let expr = expressions::BinaryExpr::new(
            Arc::new(expr),
            Operator::Or,
            Arc::new(expressions::BinaryExpr::new(
                Arc::clone(&column_c),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Float64(Some(0.0)))),
            )),
        );

        let result = adapter.rewrite(Arc::new(expr)).unwrap();
        let outer = result
            .downcast_ref::<expressions::BinaryExpr>()
            .expect("Expected outer BinaryExpr");
        assert_eq!(*outer.op(), Operator::Or);

        let left = outer
            .left()
            .downcast_ref::<expressions::BinaryExpr>()
            .expect("Expected left BinaryExpr");
        assert_eq!(*left.op(), Operator::Plus);

        let left_cast = assert_cast_expr(left.left());
        assert_eq!(left_cast.target_field().data_type(), &DataType::Int64);
        assert_cast_input_column(left_cast, "a", 0);

        let right = outer
            .right()
            .downcast_ref::<expressions::BinaryExpr>()
            .expect("Expected right BinaryExpr");
        assert_eq!(*right.op(), Operator::Gt);
        let null_literal = right
            .left()
            .downcast_ref::<Literal>()
            .expect("Expected null literal");
        assert_eq!(*null_literal.value(), ScalarValue::Float64(None));
    }

    #[test]
    fn test_rewrite_struct_column_incompatible() {
        let physical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(vec![Field::new("field1", DataType::Binary, true)].into()),
            true,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(vec![Field::new("field1", DataType::Int32, true)].into()),
            true,
        )]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("data", 0));

        let error_msg = adapter.rewrite(column_expr).unwrap_err().to_string();
        // validate_struct_compatibility provides more specific error about which field can't be cast
        assert_contains!(
            error_msg,
            "Cannot cast struct field 'field1' from type Binary to type Int32"
        );
    }

    #[test]
    fn test_rewrite_struct_compatible_cast() {
        let physical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8View, true),
                ]
                .into(),
            ),
            false,
        )]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("data", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        let logical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]
        .into();
        let logical_field = Arc::new(Field::new(
            "data",
            DataType::Struct(logical_struct_fields),
            false,
        ));

        let expected = Arc::new(CastExpr::new_with_target_field(
            Arc::new(Column::new("data", 0)),
            logical_field,
            None,
        )) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_missing_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("c", 2));

        let result = adapter.rewrite(column_expr)?;

        // Should be replaced with a literal null
        if let Some(literal) = result.downcast_ref::<Literal>() {
            assert_eq!(*literal.value(), ScalarValue::Float64(None));
        } else {
            panic!("Expected literal expression");
        }

        Ok(())
    }

    #[test]
    fn test_rewrite_missing_column_propagates_metadata() -> Result<()> {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true).with_metadata(HashMap::from([(
                "logical_meta".to_string(),
                "1".to_string(),
            )])),
        ]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema.clone()))
            .unwrap();

        let result = adapter.rewrite(Arc::new(Column::new("b", 1)))?;
        let literal = result
            .downcast_ref::<Literal>()
            .expect("Expected literal expression");

        assert_eq!(
            literal
                .return_field(physical_schema.as_ref())?
                .metadata()
                .get("logical_meta")
                .map(String::as_str),
            Some("1")
        );
        Ok(())
    }

    #[test]
    fn test_rewrite_missing_column_non_nullable_error() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false), // Missing and non-nullable
        ]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("b", 1));

        let error_msg = adapter.rewrite(column_expr).unwrap_err().to_string();
        assert_contains!(error_msg, "Non-nullable column 'b' is missing");
    }

    #[test]
    fn test_rewrite_missing_column_nullable() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true), // Missing but nullable
        ]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("b", 1));

        let result = adapter.rewrite(column_expr).unwrap();

        let expected =
            Arc::new(Literal::new(ScalarValue::Utf8(None))) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_replace_columns_with_literals() -> Result<()> {
        let partition_value = ScalarValue::Utf8(Some("test_value".to_string()));
        let replacements = HashMap::from([("partition_col", &partition_value)]);

        let column_expr =
            Arc::new(Column::new("partition_col", 0)) as Arc<dyn PhysicalExpr>;
        let result = replace_columns_with_literals(column_expr, &replacements)?;

        // Should be replaced with the partition value
        let literal = result
            .downcast_ref::<Literal>()
            .expect("Expected literal expression");
        assert_eq!(*literal.value(), partition_value);

        Ok(())
    }

    #[test]
    fn test_replace_columns_with_literals_no_match() -> Result<()> {
        let value = ScalarValue::Utf8(Some("test_value".to_string()));
        let replacements = HashMap::from([("other_col", &value)]);

        let column_expr =
            Arc::new(Column::new("partition_col", 0)) as Arc<dyn PhysicalExpr>;
        let result = replace_columns_with_literals(column_expr, &replacements)?;

        assert!(result.downcast_ref::<Column>().is_some());
        Ok(())
    }

    #[test]
    fn test_replace_columns_with_literals_nested_expr() -> Result<()> {
        let value_a = ScalarValue::Int64(Some(10));
        let value_b = ScalarValue::Int64(Some(20));
        let replacements = HashMap::from([("a", &value_a), ("b", &value_b)]);

        let expr = Arc::new(expressions::BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        )) as Arc<dyn PhysicalExpr>;

        let result = replace_columns_with_literals(expr, &replacements)?;
        assert_eq!(result.to_string(), "10 + 20");

        Ok(())
    }

    #[test]
    fn test_rewrite_no_change_needed() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let result = adapter.rewrite(Arc::clone(&column_expr))?;

        // Should be the same expression (no transformation needed)
        // We compare the underlying pointer through the trait object
        assert!(std::ptr::eq(
            std::ptr::from_ref::<dyn PhysicalExpr>(column_expr.as_ref()),
            std::ptr::from_ref::<dyn PhysicalExpr>(result.as_ref())
        ));

        Ok(())
    }

    #[test]
    fn test_non_nullable_missing_column_error() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false), // Non-nullable missing column
        ]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("b", 1));

        let result = adapter.rewrite(column_expr);
        assert!(result.is_err());
        assert_contains!(
            result.unwrap_err().to_string(),
            "Non-nullable column 'b' is missing from the physical schema"
        );
    }

    /// Helper function to project expressions onto a RecordBatch
    fn batch_project(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        batch: &RecordBatch,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let arrays = expr
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(Into::into)
        }
    }

    /// Example showing how we can use the `DefaultPhysicalExprAdapter` to adapt RecordBatches during a scan
    /// to apply projections, type conversions and handling of missing columns all at once.
    #[test]
    fn test_adapt_batches() {
        let physical_batch = record_batch!(
            ("a", Int32, vec![Some(1), None, Some(3)]),
            ("extra", Utf8, vec![Some("x"), Some("y"), None])
        )
        .unwrap();

        let physical_schema = physical_batch.schema();

        let logical_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true), // Different type
            Field::new("b", DataType::Utf8, true),  // Missing from physical
        ]));

        let projection = vec![
            col("b", &logical_schema).unwrap(),
            col("a", &logical_schema).unwrap(),
        ];

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::clone(&logical_schema), Arc::clone(&physical_schema))
            .unwrap();

        let adapted_projection = projection
            .into_iter()
            .map(|expr| adapter.rewrite(expr).unwrap())
            .collect_vec();

        let adapted_schema = Arc::new(Schema::new(
            adapted_projection
                .iter()
                .map(|expr| expr.return_field(&physical_schema).unwrap())
                .collect_vec(),
        ));

        let res = batch_project(
            adapted_projection,
            &physical_batch,
            Arc::clone(&adapted_schema),
        )
        .unwrap();

        assert_eq!(res.num_columns(), 2);
        assert_eq!(res.column(0).data_type(), &DataType::Utf8);
        assert_eq!(res.column(1).data_type(), &DataType::Int64);
        assert_eq!(
            res.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect_vec(),
            vec![None, None, None]
        );
        assert_eq!(
            res.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect_vec(),
            vec![Some(1), None, Some(3)]
        );
    }

    /// Test that struct columns are properly adapted including:
    /// - Type casting of subfields (Int32 -> Int64, Utf8 -> Utf8View)
    /// - Missing fields in logical schema are filled with nulls
    #[test]
    fn test_adapt_struct_batches() {
        // Physical struct: {id: Int32, name: Utf8}
        let physical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();

        let struct_array = StructArray::new(
            physical_struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    None,
                    Some("charlie"),
                ])) as _,
            ],
            None,
        );

        let physical_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(physical_struct_fields),
            false,
        )]));

        let physical_batch = RecordBatch::try_new(
            Arc::clone(&physical_schema),
            vec![Arc::new(struct_array)],
        )
        .unwrap();

        // Logical struct: {id: Int64, name: Utf8View, extra: Boolean}
        // - id: cast from Int32 to Int64
        // - name: cast from Utf8 to Utf8View
        // - extra: missing from physical, should be filled with nulls
        let logical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("extra", DataType::Boolean, true), // New field, not in physical
        ]
        .into();

        let logical_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(logical_struct_fields),
            false,
        )]));

        let projection = vec![col("data", &logical_schema).unwrap()];

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::clone(&logical_schema), Arc::clone(&physical_schema))
            .unwrap();

        let adapted_projection = projection
            .into_iter()
            .map(|expr| adapter.rewrite(expr).unwrap())
            .collect_vec();

        let adapted_schema = Arc::new(Schema::new(
            adapted_projection
                .iter()
                .map(|expr| expr.return_field(&physical_schema).unwrap())
                .collect_vec(),
        ));

        let res = batch_project(
            adapted_projection,
            &physical_batch,
            Arc::clone(&adapted_schema),
        )
        .unwrap();

        assert_eq!(res.num_columns(), 1);

        let result_struct = res
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Verify id field is cast to Int64
        let id_col = result_struct.column_by_name("id").unwrap();
        assert_eq!(id_col.data_type(), &DataType::Int64);
        let id_values = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            id_values.iter().collect_vec(),
            vec![Some(1), Some(2), Some(3)]
        );

        // Verify name field is cast to Utf8View
        let name_col = result_struct.column_by_name("name").unwrap();
        assert_eq!(name_col.data_type(), &DataType::Utf8View);
        let name_values = name_col.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(
            name_values.iter().collect_vec(),
            vec![Some("alice"), None, Some("charlie")]
        );

        // Verify extra field (missing from physical) is filled with nulls
        let extra_col = result_struct.column_by_name("extra").unwrap();
        assert_eq!(extra_col.data_type(), &DataType::Boolean);
        let extra_values = extra_col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(extra_values.iter().collect_vec(), vec![None, None, None]);
    }

    /// Test that List<Struct> columns are properly adapted with struct evolution.
    #[test]
    fn test_adapt_list_struct_batches() {
        // Physical: List<{id: Int32, name: Utf8}>
        let physical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();

        let struct_array = StructArray::new(
            physical_struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    None,
                    Some("charlie"),
                ])) as _,
            ],
            None,
        );

        // One list element per row
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(physical_struct_fields.clone()),
            true,
        ));
        let offsets =
            arrow::buffer::OffsetBuffer::from_lengths(vec![1usize; struct_array.len()]);
        let list_array = GenericListArray::<i32>::new(
            item_field,
            offsets,
            Arc::new(struct_array),
            None,
        );

        let physical_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(physical_struct_fields),
                true,
            ))),
            false,
        )]));

        let physical_batch = RecordBatch::try_new(
            Arc::clone(&physical_schema),
            vec![Arc::new(list_array)],
        )
        .unwrap();

        // Logical: List<{id: Int64, name: Utf8View, extra: Boolean}>
        let logical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("extra", DataType::Boolean, true),
        ]
        .into();

        let logical_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(logical_struct_fields.clone()),
                true,
            ))),
            false,
        )]));

        let projection = vec![col("data", &logical_schema).unwrap()];

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::clone(&logical_schema), Arc::clone(&physical_schema))
            .unwrap();

        let adapted_projection = projection
            .into_iter()
            .map(|expr| adapter.rewrite(expr).unwrap())
            .collect_vec();

        let adapted_schema = Arc::new(Schema::new(
            adapted_projection
                .iter()
                .map(|expr| expr.return_field(&physical_schema).unwrap())
                .collect_vec(),
        ));

        let res = batch_project(
            adapted_projection,
            &physical_batch,
            Arc::clone(&adapted_schema),
        )
        .unwrap();

        assert_eq!(res.num_columns(), 1);

        let result_list = res
            .column(0)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .unwrap();

        // Check each list element contains the evolved struct
        assert_eq!(result_list.len(), 3);
        let flat_structs = result_list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let id_col = flat_structs.column_by_name("id").unwrap();
        assert_eq!(id_col.data_type(), &DataType::Int64);
        let id_values = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            id_values.iter().collect_vec(),
            vec![Some(1), Some(2), Some(3)]
        );

        let name_col = flat_structs.column_by_name("name").unwrap();
        assert_eq!(name_col.data_type(), &DataType::Utf8View);
        let name_values = name_col.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(
            name_values.iter().collect_vec(),
            vec![Some("alice"), None, Some("charlie")]
        );

        let extra_col = flat_structs.column_by_name("extra").unwrap();
        assert_eq!(extra_col.data_type(), &DataType::Boolean);
        let extra_values = extra_col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(extra_values.iter().collect_vec(), vec![None, None, None]);
    }

    #[test]
    fn test_try_rewrite_struct_field_access() {
        // Test the core logic of try_rewrite_struct_field_access
        let physical_schema = Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Field::new("existing_field", DataType::Int32, true)].into(),
            ),
            true,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![
                    Field::new("existing_field", DataType::Int32, true),
                    Field::new("missing_field", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        )]);

        let rewriter = DefaultPhysicalExprAdapterRewriter {
            logical_file_schema: Arc::new(logical_schema),
            physical_file_schema: Arc::new(physical_schema),
            generated_struct_casts: HashMap::new(),
        };

        // Test that when a field exists in physical schema, it returns None
        let column = Arc::new(Column::new("struct_col", 0)) as Arc<dyn PhysicalExpr>;
        let result = rewriter.try_rewrite_struct_field_access(&column).unwrap();
        assert!(result.is_none());

        // The actual test for the get_field expression would require creating a proper ScalarFunctionExpr
        // with ScalarUDF, which is complex to set up in a unit test. The integration tests in
        // datafusion/core/tests/parquet/schema_adapter.rs provide better coverage for this functionality.
    }

    /// Build `get_field(column, 'field')` against `schema`.
    fn get_field_expr(
        schema: &Schema,
        column: &str,
        field: &str,
    ) -> Arc<dyn PhysicalExpr> {
        let index = schema.index_of(column).unwrap();
        Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(datafusion_expr::ScalarUDF::from(GetFieldFunc::new())),
                vec![
                    Arc::new(Column::new(column, index)),
                    Arc::new(Literal::new(ScalarValue::from(field))),
                ],
                schema,
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )
            .unwrap(),
        )
    }

    fn struct_schemas(
        physical_fields: Vec<Field>,
        logical_fields: Vec<Field>,
    ) -> (SchemaRef, SchemaRef) {
        let physical = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(physical_fields.into()),
            true,
        )]));
        let logical = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(logical_fields.into()),
            true,
        )]));
        (logical, physical)
    }

    fn decimal_cast_leaf_types(data_type: DataType) -> Vec<DataType> {
        let item = Arc::new(Field::new("item", data_type.clone(), true));
        vec![
            data_type.clone(),
            DataType::List(Arc::clone(&item)),
            DataType::LargeList(Arc::clone(&item)),
            DataType::FixedSizeList(Arc::clone(&item), 2),
            DataType::ListView(Arc::clone(&item)),
            DataType::LargeListView(item),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", data_type.clone(), true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(data_type.clone())),
            DataType::new_list(
                DataType::Struct(
                    vec![Field::new("value", data_type.clone(), true)].into(),
                ),
                true,
            ),
            DataType::new_list(DataType::new_list(data_type, true), true),
        ]
    }

    #[test]
    fn test_narrow_struct_cast_preserves_struct_unwrapping() -> Result<()> {
        use arrow::array::{
            ArrayRef, Decimal128Array, DictionaryArray, Int8Array, ListArray,
        };
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Int8Type;

        let values = Arc::new(StructArray::new(
            vec![Field::new("value", DataType::Int32, true)].into(),
            vec![Arc::new(Int32Array::from(vec![1]))],
            None,
        )) as ArrayRef;
        let dictionary = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            values,
        )?) as ArrayRef;
        let expected_struct = Arc::new(StructArray::new(
            vec![Field::new("value", DataType::Decimal128(10, 2), true)].into(),
            vec![Arc::new(
                Decimal128Array::from(vec![100]).with_precision_and_scale(10, 2)?,
            )],
            None,
        )) as ArrayRef;
        let wrap_list = |values: ArrayRef| -> ArrayRef {
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", values.data_type().clone(), true)),
                OffsetBuffer::from_lengths([1]),
                values,
                None,
            ))
        };
        let wrap_dictionary = |values: ArrayRef| -> ArrayRef {
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), values)
                    .unwrap(),
            )
        };
        for (label, physical, expected) in [
            (
                "direct Dictionary",
                Arc::clone(&dictionary),
                Arc::clone(&expected_struct),
            ),
            (
                "List of Dictionary",
                wrap_list(Arc::clone(&dictionary)),
                wrap_list(Arc::clone(&expected_struct)),
            ),
            (
                "Dictionary of Dictionary",
                wrap_dictionary(dictionary),
                wrap_dictionary(expected_struct),
            ),
        ] {
            let (logical_schema, physical_schema) = struct_schemas(
                vec![Field::new("x", physical.data_type().clone(), true)],
                vec![Field::new("x", expected.data_type().clone(), true)],
            );
            let DataType::Struct(fields) = physical_schema.field(0).data_type() else {
                unreachable!()
            };
            let batch = RecordBatch::try_new(
                Arc::clone(&physical_schema),
                vec![Arc::new(StructArray::new(
                    fields.clone(),
                    vec![physical],
                    None,
                ))],
            )?;
            let adapter = DefaultPhysicalExprAdapterFactory
                .create(Arc::clone(&logical_schema), physical_schema)?;
            let rewritten = adapter.rewrite(get_field_expr(&logical_schema, "s", "x"))?;
            let actual = rewritten.evaluate(&batch)?.into_array(1)?;
            assert_eq!(actual.to_data(), expected.to_data(), "{label}");
        }
        Ok(())
    }

    /// `s['x']` where the file stores `x` as `Int32` and the table declares
    /// `Int64` must cast the extracted field, not the whole struct, so that
    /// the column stays visible under the `get_field`.
    ///
    /// See <https://github.com/apache/datafusion/issues/24109>.
    #[test]
    fn test_narrow_struct_cast_to_field_access() {
        for (physical_type, logical_type) in [
            (DataType::Int32, DataType::Int64),
            (
                DataType::new_list(DataType::Int32, true),
                DataType::new_list(DataType::Int64, true),
            ),
        ] {
            let (logical_schema, physical_schema) = struct_schemas(
                vec![Field::new("x", physical_type.clone(), true)],
                vec![Field::new("x", logical_type.clone(), true)],
            );

            let adapter = DefaultPhysicalExprAdapterFactory
                .create(Arc::clone(&logical_schema), physical_schema)
                .unwrap();
            let rewritten = adapter
                .rewrite(get_field_expr(&logical_schema, "s", "x"))
                .unwrap();

            let cast = assert_cast_expr(&rewritten);
            assert_eq!(cast.cast_type(), &logical_type);
            let get_field = cast
                .expr()
                .downcast_ref::<ScalarFunctionExpr>()
                .expect("Expected get_field under the cast");
            assert_eq!(get_field.return_type(), &physical_type);
            assert!(
                get_field.args()[0].downcast_ref::<Column>().is_some(),
                "the struct column must not be hidden behind a cast, got: {rewritten}"
            );
        }
    }

    /// Selecting one field of an explicit cast must still evaluate sibling
    /// conversions, even when schema adaptation inserts another cast below it.
    #[test]
    fn test_narrow_struct_cast_preserves_explicit_cast_errors() -> Result<()> {
        use arrow::array::{ArrayRef, Int16Array};

        for adapt_input in [false, true] {
            let physical_x_type = if adapt_input {
                DataType::Int16
            } else {
                DataType::Int32
            };
            let (logical_schema, physical_schema) = struct_schemas(
                vec![
                    Field::new("x", physical_x_type, true),
                    Field::new("y", DataType::Utf8, true),
                ],
                vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Utf8, true),
                ],
            );
            let DataType::Struct(physical_fields) = physical_schema.field(0).data_type()
            else {
                unreachable!()
            };
            let x: ArrayRef = if adapt_input {
                Arc::new(Int16Array::from(vec![1]))
            } else {
                Arc::new(Int32Array::from(vec![1]))
            };
            let batch = RecordBatch::try_new(
                Arc::clone(&physical_schema),
                vec![Arc::new(StructArray::new(
                    physical_fields.clone(),
                    vec![x, Arc::new(StringArray::from(vec!["bad"]))],
                    None,
                ))],
            )?;
            let user_cast = Arc::new(CastExpr::new(
                Arc::new(Column::new("s", 0)),
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int32, true),
                        Field::new("y", DataType::Int32, true),
                    ]
                    .into(),
                ),
                None,
            ));
            let expr = Arc::new(ScalarFunctionExpr::try_new(
                Arc::new(datafusion_expr::ScalarUDF::from(GetFieldFunc::new())),
                vec![user_cast, Arc::new(Literal::new(ScalarValue::from("x")))],
                &logical_schema,
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )?) as Arc<dyn PhysicalExpr>;
            let original_error = expr.evaluate(&batch).unwrap_err().to_string();
            assert_contains!(original_error, "While casting struct field 'y'");

            let adapter = DefaultPhysicalExprAdapterFactory
                .create(logical_schema, physical_schema)?;
            let rewritten = adapter.rewrite(expr)?;
            let error = rewritten.evaluate(&batch).unwrap_err().to_string();
            assert_contains!(error, "While casting struct field 'y'");
        }
        Ok(())
    }

    /// The result inherits nullability from every parent, not just the leaf.
    /// Equal leaf fields do not justify dropping a cast if that loses the
    /// logical return field's nullability.
    #[test]
    fn test_narrow_struct_cast_preserves_logical_return_field() -> Result<()> {
        let metadata = HashMap::from([("logical_meta".to_string(), "1".to_string())]);
        for nested in [false, true] {
            let schema = |leaf: Field, parent_nullable| {
                let (field, root_nullable) = if nested {
                    (
                        Field::new(
                            "inner",
                            DataType::Struct(vec![leaf].into()),
                            parent_nullable,
                        ),
                        false,
                    )
                } else {
                    (leaf, parent_nullable)
                };
                Arc::new(Schema::new(vec![Field::new(
                    "s",
                    DataType::Struct(vec![field].into()),
                    root_nullable,
                )]))
            };
            for same_leaf_type in [false, true] {
                let physical_leaf = Field::new("x", DataType::Int32, false)
                    .with_metadata(metadata.clone());
                let logical_leaf = if same_leaf_type {
                    physical_leaf.clone()
                } else {
                    Field::new("x", DataType::Int64, false).with_metadata(HashMap::from(
                        [("logical_meta".to_string(), "2".to_string())],
                    ))
                };
                let physical_schema = schema(physical_leaf, false);
                let logical_schema = schema(logical_leaf, true);
                let mut args: Vec<Arc<dyn PhysicalExpr>> =
                    vec![Arc::new(Column::new("s", 0))];
                if nested {
                    args.push(Arc::new(Literal::new(ScalarValue::from("inner"))));
                }
                args.push(Arc::new(Literal::new(ScalarValue::from("x"))));
                let expr = Arc::new(ScalarFunctionExpr::try_new(
                    Arc::new(datafusion_expr::ScalarUDF::from(GetFieldFunc::new())),
                    args,
                    &logical_schema,
                    Arc::new(datafusion_common::config::ConfigOptions::default()),
                )?) as Arc<dyn PhysicalExpr>;
                let expected_field = expr.return_field(&logical_schema)?;
                assert!(expected_field.is_nullable());

                let adapter = DefaultPhysicalExprAdapterFactory
                    .create(logical_schema, Arc::clone(&physical_schema))?;
                let rewritten = adapter.rewrite(expr)?;
                assert_eq!(
                    rewritten.return_field(&physical_schema)?,
                    expected_field,
                    "nested={nested}, same_leaf_type={same_leaf_type}"
                );
                assert!(rewritten.nullable(&physical_schema)?);
            }
        }
        Ok(())
    }

    /// Some decimal casts can fail while preparing the conversion, even for
    /// an entirely null input. An all-null Struct skips its child conversions.
    #[test]
    fn test_narrow_struct_cast_preserves_all_null_decimal_casts() -> Result<()> {
        use arrow::array::new_null_array;
        use arrow::buffer::NullBuffer;
        use arrow::datatypes::{UnionFields, UnionMode};

        for (physical_type, logical_type) in [
            (DataType::Decimal128(38, -38), DataType::Decimal128(38, 38)),
            (DataType::Utf8, DataType::Decimal128(10, -1)),
            (DataType::Decimal128(38, -39), DataType::Int64),
        ]
        .into_iter()
        .flat_map(|(physical, logical)| {
            decimal_cast_leaf_types(physical)
                .into_iter()
                .zip(decimal_cast_leaf_types(logical))
        })
        // A directly decimal target must still retain the ancestor even when
        // an unrelated Union arm contains a Struct.
        .chain([(
            DataType::Union(
                UnionFields::try_new(
                    [0, 1],
                    [
                        Field::new("string", DataType::Utf8, true),
                        Field::new(
                            "struct",
                            DataType::Struct(
                                vec![Field::new("z", DataType::Int32, true)].into(),
                            ),
                            true,
                        ),
                    ],
                )?,
                UnionMode::Dense,
            ),
            DataType::Decimal128(10, -1),
        )]) {
            let (logical_schema, physical_schema) = struct_schemas(
                vec![Field::new("x", physical_type.clone(), true)],
                vec![Field::new("x", logical_type, true)],
            );
            let DataType::Struct(physical_fields) = physical_schema.field(0).data_type()
            else {
                unreachable!()
            };
            let batch = RecordBatch::try_new(
                Arc::clone(&physical_schema),
                vec![Arc::new(StructArray::new(
                    physical_fields.clone(),
                    vec![new_null_array(&physical_type, 2)],
                    Some(NullBuffer::new_null(2)),
                ))],
            )?;
            let adapter = DefaultPhysicalExprAdapterFactory
                .create(Arc::clone(&logical_schema), physical_schema)?;
            let expr = get_field_expr(&logical_schema, "s", "x");

            // Establish the result of the original whole-struct conversion.
            let whole_struct_cast = adapter.rewrite(Arc::new(Column::new("s", 0)))?;
            let original = Arc::clone(&expr).with_new_children(vec![
                whole_struct_cast,
                Arc::new(Literal::new(ScalarValue::from("x"))),
            ])?;
            let expected = original.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(expected.null_count(), batch.num_rows());

            let rewritten = adapter.rewrite(expr)?;
            let result = rewritten.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(result.to_data(), expected.to_data());
        }
        Ok(())
    }

    #[test]
    fn test_narrow_struct_cast_keeps_matching_decimal_fields_optimized() -> Result<()> {
        for (decimal_type, change_metadata) in
            decimal_cast_leaf_types(DataType::Decimal128(10, -1))
                .into_iter()
                .flat_map(|data_type| [(data_type.clone(), false), (data_type, true)])
        {
            let physical_field = Field::new("x", decimal_type.clone(), true);
            let logical_field = if change_metadata {
                physical_field.clone().with_metadata(HashMap::from([(
                    "logical_meta".to_string(),
                    "1".to_string(),
                )]))
            } else {
                physical_field.clone()
            };
            let (logical_schema, physical_schema) = struct_schemas(
                vec![physical_field, Field::new("y", DataType::Int32, true)],
                vec![logical_field, Field::new("y", DataType::Int64, true)],
            );
            let expr = get_field_expr(&logical_schema, "s", "x");
            let expected_field = expr.return_field(&logical_schema)?;
            let adapter = DefaultPhysicalExprAdapterFactory
                .create(logical_schema, Arc::clone(&physical_schema))?;
            let rewritten = adapter.rewrite(expr)?;
            assert_eq!(rewritten.return_field(&physical_schema)?, expected_field);
            let extracted = if change_metadata {
                assert_cast_expr(&rewritten).expr()
            } else {
                &rewritten
            };
            let get_field = extracted.downcast_ref::<ScalarFunctionExpr>().unwrap();
            assert!(get_field.args()[0].downcast_ref::<Column>().is_some());
        }
        Ok(())
    }

    #[test]
    fn test_narrow_decimal_struct_cast_ignores_siblings() -> Result<()> {
        use arrow::array::ArrayRef;
        use datafusion_physical_expr::planner::logical2physical;

        for (nested, list_leaf) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let (physical_type, logical_type, x, expected) = if list_leaf {
                (
                    DataType::new_list(DataType::Int32, true),
                    DataType::new_list(DataType::Decimal128(10, 2), true),
                    ScalarValue::new_list(
                        &[ScalarValue::Int32(Some(1))],
                        &DataType::Int32,
                        true,
                    ) as ArrayRef,
                    ScalarValue::List(ScalarValue::new_list(
                        &[ScalarValue::Decimal128(Some(100), 10, 2)],
                        &DataType::Decimal128(10, 2),
                        true,
                    )),
                )
            } else {
                (
                    DataType::Int32,
                    DataType::Decimal128(10, 2),
                    Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                    ScalarValue::Decimal128(Some(100), 10, 2),
                )
            };
            let mut physical_fields = vec![
                Field::new("x", physical_type, true),
                Field::new("y", DataType::Utf8, true),
            ];
            let mut logical_fields = vec![
                Field::new("x", logical_type, true).with_metadata(HashMap::from([(
                    "logical_meta".to_string(),
                    "1".to_string(),
                )])),
                Field::new("y", DataType::Int32, true),
            ];
            let mut column = Arc::new(StructArray::new(
                physical_fields.clone().into(),
                vec![x, Arc::new(StringArray::from(vec!["bad"]))],
                None,
            )) as ArrayRef;
            let mut args = vec![datafusion_expr::col("s")];
            if nested {
                physical_fields = vec![
                    Field::new("inner", DataType::Struct(physical_fields.into()), true),
                    Field::new("y", DataType::Utf8, true),
                ];
                logical_fields = vec![
                    Field::new("inner", DataType::Struct(logical_fields.into()), true),
                    Field::new("y", DataType::Int32, true),
                ];
                column = Arc::new(StructArray::new(
                    physical_fields.clone().into(),
                    vec![column, Arc::new(StringArray::from(vec!["bad"]))],
                    None,
                ));
                args.push(datafusion_expr::lit("inner"));
            }
            args.push(datafusion_expr::lit("x"));
            let (logical_schema, physical_schema) =
                struct_schemas(physical_fields, logical_fields);
            let expr = logical2physical(
                &datafusion_functions::core::get_field().call(args),
                &logical_schema,
            );
            let expected_field = expr.return_field(&logical_schema)?;
            let batch = RecordBatch::try_new(Arc::clone(&physical_schema), vec![column])?;
            let adapter = DefaultPhysicalExprAdapterFactory
                .create(logical_schema, Arc::clone(&physical_schema))?;
            let rewritten = adapter.rewrite(expr)?;
            assert_eq!(rewritten.return_field(&physical_schema)?, expected_field);
            let values = rewritten.evaluate(&batch)?.into_array(1)?;
            assert_eq!(
                ScalarValue::try_from_array(&values, 0)?,
                expected,
                "nested={nested}, list_leaf={list_leaf}"
            );
        }
        Ok(())
    }

    /// A struct field that only differs in a nested leaf type still ends up
    /// with a single cast on the extracted field.
    #[test]
    fn test_narrow_struct_cast_nested_field_access() {
        let (logical_schema, physical_schema) = struct_schemas(
            vec![Field::new(
                "inner",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Utf8, true),
                        Field::new("y", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            )],
            vec![Field::new(
                "inner",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Utf8View, true),
                        Field::new("y", DataType::Decimal128(10, -1), true),
                    ]
                    .into(),
                ),
                true,
            )],
        );

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema)
            .unwrap();
        let outer = get_field_expr(&logical_schema, "s", "inner");
        let expr = Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(datafusion_expr::ScalarUDF::from(GetFieldFunc::new())),
                vec![outer, Arc::new(Literal::new(ScalarValue::from("x")))],
                &logical_schema,
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;

        let rewritten = adapter.rewrite(expr).unwrap();

        let cast = assert_cast_expr(&rewritten);
        assert_eq!(cast.cast_type(), &DataType::Utf8View);
        let outer_get_field = cast
            .expr()
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("Expected get_field under the cast");
        let inner_get_field = outer_get_field.args()[0]
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("Expected a nested get_field");
        assert!(
            inner_get_field.args()[0].downcast_ref::<Column>().is_some(),
            "the struct column must not be hidden behind a cast, got: {rewritten}"
        );
    }

    /// A struct column that needs no adaptation at all is left completely
    /// alone — the narrowing must not disturb the common case.
    #[test]
    fn test_narrow_struct_cast_leaves_matching_schema_alone() {
        let (logical_schema, physical_schema) = struct_schemas(
            vec![Field::new("x", DataType::Int32, true)],
            vec![Field::new("x", DataType::Int32, true)],
        );

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema)
            .unwrap();
        let expr = get_field_expr(&logical_schema, "s", "x");

        let rewritten = adapter.rewrite(Arc::clone(&expr)).unwrap();

        assert_eq!(
            rewritten.to_string(),
            expr.to_string(),
            "an unadapted struct column must pass through untouched"
        );
    }

    /// When the accessed field has the same type in both schemas, the struct
    /// cast disappears entirely rather than being replaced by a field cast:
    /// only a sibling field forced the column-level cast in the first place.
    #[test]
    fn test_narrow_struct_cast_drops_cast_when_field_types_match() {
        let (logical_schema, physical_schema) = struct_schemas(
            vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Int32, true),
            ],
            vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Int64, true),
            ],
        );

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema)
            .unwrap();
        let expr = Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(datafusion_expr::ScalarUDF::from(GetFieldFunc::new())),
                vec![
                    Arc::new(Column::new("s", 0)),
                    Arc::new(Literal::new(ScalarValue::from("x"))),
                ],
                &logical_schema,
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;

        let rewritten = adapter.rewrite(expr).unwrap();

        assert!(
            rewritten.downcast_ref::<CastExpr>().is_none(),
            "`x` has the same type in both schemas, so no cast is needed, got: {rewritten}"
        );
        let get_field = rewritten
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("Expected a bare get_field");
        assert_eq!(get_field.return_type(), &DataType::Int32);
        assert!(
            get_field.args()[0].downcast_ref::<Column>().is_some(),
            "the struct column must not be hidden behind a cast, got: {rewritten}"
        );
    }

    /// `s['inner']['x']` is simplified to the flattened `get_field(s, 'inner',
    /// 'x')`, so the whole key path has to be resolved.
    #[test]
    fn test_narrow_struct_cast_flattened_field_path() {
        let (logical_schema, physical_schema) = struct_schemas(
            vec![Field::new(
                "inner",
                DataType::Struct(vec![Field::new("x", DataType::Utf8, true)].into()),
                true,
            )],
            vec![Field::new(
                "inner",
                DataType::Struct(vec![Field::new("x", DataType::Utf8View, true)].into()),
                true,
            )],
        );

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema)
            .unwrap();
        let expr = Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(datafusion_expr::ScalarUDF::from(GetFieldFunc::new())),
                vec![
                    Arc::new(Column::new("s", 0)),
                    Arc::new(Literal::new(ScalarValue::from("inner"))),
                    Arc::new(Literal::new(ScalarValue::from("x"))),
                ],
                &logical_schema,
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;

        let rewritten = adapter.rewrite(expr).unwrap();

        let cast = assert_cast_expr(&rewritten);
        assert_eq!(cast.cast_type(), &DataType::Utf8View);
        let get_field = cast
            .expr()
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("Expected get_field under the cast");
        assert_eq!(get_field.return_type(), &DataType::Utf8);
        assert_eq!(
            get_field.args().len(),
            3,
            "the full key path must be preserved, got: {rewritten}"
        );
        assert!(
            get_field.args()[0].downcast_ref::<Column>().is_some(),
            "the struct column must not be hidden behind a cast, got: {rewritten}"
        );
    }

    /// A key path whose leaf is missing from the file still resolves to a
    /// typed null literal.
    #[test]
    fn test_narrow_struct_cast_flattened_field_path_missing_leaf() {
        let (logical_schema, physical_schema) = struct_schemas(
            vec![Field::new(
                "inner",
                DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()),
                true,
            )],
            vec![Field::new(
                "inner",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int32, true),
                        Field::new("y", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            )],
        );

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema)
            .unwrap();
        let expr = Arc::new(
            ScalarFunctionExpr::try_new(
                Arc::new(datafusion_expr::ScalarUDF::from(GetFieldFunc::new())),
                vec![
                    Arc::new(Column::new("s", 0)),
                    Arc::new(Literal::new(ScalarValue::from("inner"))),
                    Arc::new(Literal::new(ScalarValue::from("y"))),
                ],
                &logical_schema,
                Arc::new(datafusion_common::config::ConfigOptions::default()),
            )
            .unwrap(),
        ) as Arc<dyn PhysicalExpr>;

        let rewritten = adapter.rewrite(expr).unwrap();

        let literal = rewritten
            .downcast_ref::<Literal>()
            .expect("Expected a null literal");
        assert_eq!(*literal.value(), ScalarValue::Utf8(None));
    }

    /// Accessing a field the file does not have yields a typed null literal.
    #[test]
    fn test_narrow_struct_cast_missing_field() {
        let (logical_schema, physical_schema) = struct_schemas(
            vec![Field::new("x", DataType::Int32, true)],
            vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Utf8, true),
            ],
        );

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema)
            .unwrap();
        let rewritten = adapter
            .rewrite(get_field_expr(&logical_schema, "s", "y"))
            .unwrap();

        let literal = rewritten
            .downcast_ref::<Literal>()
            .expect("Expected a null literal");
        assert_eq!(*literal.value(), ScalarValue::Utf8(None));
    }

    /// `get_field` on a Map column is a runtime key lookup, not a schema-level
    /// field access, so the map value must keep its cast.
    #[test]
    fn test_map_field_access_keeps_cast() {
        let map_type = |value_type: DataType| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", value_type, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        };
        let physical_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            map_type(DataType::Int32),
            true,
        )]));
        let logical_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            map_type(DataType::Int64),
            true,
        )]));

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::clone(&logical_schema), physical_schema)
            .unwrap();
        let rewritten = adapter
            .rewrite(get_field_expr(&logical_schema, "s", "k"))
            .unwrap();

        let get_field = rewritten
            .downcast_ref::<ScalarFunctionExpr>()
            .expect("Expected the get_field to be preserved");
        assert!(
            get_field.args()[0].downcast_ref::<CastExpr>().is_some(),
            "map columns must keep the whole-column cast, got: {rewritten}"
        );
    }

    // ============================================================================
    // BatchAdapterFactory and BatchAdapter tests
    // ============================================================================

    #[test]
    fn test_batch_adapter_factory_basic() {
        // Target schema
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        // Source schema with different column order and type
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false), // Int32 -> Int64
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
        let adapter = factory.make_adapter(&source_schema).unwrap();

        // Create source batch
        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let adapted = adapter.adapt_batch(&source_batch).unwrap();

        // Verify schema matches target
        assert_eq!(adapted.num_columns(), 2);
        assert_eq!(adapted.schema().field(0).name(), "a");
        assert_eq!(adapted.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(adapted.schema().field(1).name(), "b");
        assert_eq!(adapted.schema().field(1).data_type(), &DataType::Utf8);

        // Verify data
        let col_a = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col_a.iter().collect_vec(), vec![Some(1), Some(2), Some(3)]);

        let col_b = adapted
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            col_b.iter().collect_vec(),
            vec![Some("hello"), None, Some("world")]
        );
    }

    #[test]
    fn test_batch_adapter_factory_missing_column() {
        // Target schema with a column missing from source
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true), // exists in source
            Field::new("c", DataType::Float64, true), // missing from source
        ]));

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
        let adapter = factory.make_adapter(&source_schema).unwrap();

        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let adapted = adapter.adapt_batch(&source_batch).unwrap();

        assert_eq!(adapted.num_columns(), 3);

        // Missing column should be filled with nulls
        let col_c = adapted.column(2);
        assert_eq!(col_c.data_type(), &DataType::Float64);
        assert_eq!(col_c.null_count(), 2); // All nulls
    }

    #[test]
    fn test_batch_adapter_factory_with_struct() {
        // Target has struct with Int64 id
        let target_struct_fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(target_struct_fields),
            false,
        )]));

        // Source has struct with Int32 id
        let source_struct_fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(source_struct_fields.clone()),
            false,
        )]));

        let struct_array = StructArray::new(
            source_struct_fields,
            vec![
                Arc::new(Int32Array::from(vec![10, 20])) as _,
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
            ],
            None,
        );

        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(struct_array)],
        )
        .unwrap();

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
        let adapter = factory.make_adapter(&source_schema).unwrap();
        let adapted = adapter.adapt_batch(&source_batch).unwrap();

        let result_struct = adapted
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Verify id was cast to Int64
        let id_col = result_struct.column_by_name("id").unwrap();
        assert_eq!(id_col.data_type(), &DataType::Int64);
        let id_values = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id_values.iter().collect_vec(), vec![Some(10), Some(20)]);
    }

    #[test]
    fn test_batch_adapter_factory_identity() {
        // When source and target schemas are identical, should pass through efficiently
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&schema));
        let adapter = factory.make_adapter(&schema).unwrap();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let adapted = adapter.adapt_batch(&batch).unwrap();

        assert_eq!(adapted.num_columns(), 2);
        assert_eq!(adapted.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(adapted.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_batch_adapter_factory_reuse() {
        // Factory can create multiple adapters for different source schemas
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, true),
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));

        // First source schema
        let source1 = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
        ]));
        let adapter1 = factory.make_adapter(&source1).unwrap();

        // Second source schema (different order)
        let source2 = Arc::new(Schema::new(vec![
            Field::new("y", DataType::Utf8, true),
            Field::new("x", DataType::Int64, false),
        ]));
        let adapter2 = factory.make_adapter(&source2).unwrap();

        // Both should work correctly
        assert!(format!("{adapter1:?}").contains("BatchAdapter"));
        assert!(format!("{adapter2:?}").contains("BatchAdapter"));
    }

    #[test]
    fn test_rewrite_column_index_and_type_mismatch() {
        let physical_schema = Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false), // Index 1
        ]);

        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false), // Index 0, Different Type
            Field::new("b", DataType::Utf8, true),
        ]);

        let adapter = DefaultPhysicalExprAdapterFactory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();

        // Logical column "a" is at index 0
        let column_expr = Arc::new(Column::new("a", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        // Should be a CastExpr
        let cast_expr = assert_cast_expr(&result);

        // Verify the inner column points to the correct physical index (1)
        assert_cast_input_column(cast_expr, "a", 1);

        // Verify cast types
        assert_eq!(
            cast_expr.data_type(&Schema::empty()).unwrap(),
            DataType::Int64
        );
    }

    #[test]
    fn test_rewrite_resolves_physical_column_by_name_before_casting() {
        let (logical_schema, physical_schema) = stale_index_cast_schemas();
        let adapter = DefaultPhysicalExprAdapterFactory
            .create(logical_schema, physical_schema)
            .unwrap();

        // Deliberately provide the wrong index for column `a`.
        // Regression: this must still resolve against physical field `a` by name.
        let rewritten = adapter.rewrite(Arc::new(Column::new("a", 0))).unwrap();
        let cast_expr = assert_cast_expr(&rewritten);
        assert_cast_input_column(cast_expr, "a", 1);
        assert_eq!(cast_expr.target_field().data_type(), &DataType::Int64);
    }
}
