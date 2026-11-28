// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Statistics for expressions, such as struct field and variant accesses,
//! read from Parquet leaf column statistics.
//!
//! ```text
//! PruningPredicate                         Parquet scan, one file
//!   asks the matcher: is `expr` a key? --> FileStatisticsResolvers::bind
//!     keeps the binding in StatsExpr  <--    -> first resolver whose `bind`
//!                                               returns a binding
//!   min_values_for_expr(stats_expr) ----> RowGroupPruningStatistics / PagesPruningStatistics
//!                                            -> the binding reads leaf
//!                                               statistics through
//!                                               ParquetLeafStatistics
//! ```
//!
//! A resolver works on [`ParquetLeafStatistics`], so the same resolver serves
//! row group statistics and the page index.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::ScalarValue;
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion_pruning::{StatisticsExprMatcher, StatsExpr};
use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};

/// Which statistic a [`BoundExprStatistics`] is asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprStatisticsKind {
    /// Lower bounds, one per container.
    Min,
    /// Upper bounds, one per container.
    Max,
    /// The number of rows where the expression is null, one per container.
    NullCount,
}

/// Statistics of Parquet leaf columns for a set of containers: the row
/// groups of a file, or the pages of one row group.
pub trait ParquetLeafStatistics {
    /// Minimum values of the Parquet leaf column `leaf`, converted to the
    /// type of `field`. Returns `None` when the statistics are unknown or
    /// cannot be trusted.
    fn leaf_mins(&self, leaf: usize, field: &Field) -> Option<ArrayRef>;

    /// Maximum values of the Parquet leaf column `leaf`. See
    /// [`Self::leaf_mins`].
    fn leaf_maxes(&self, leaf: usize, field: &Field) -> Option<ArrayRef>;

    /// Null counts of the Parquet leaf column `leaf`, as a `UInt64Array`.
    /// For a leaf with no repeated ancestor, a row counts as null when the
    /// leaf or any ancestor is null.
    fn leaf_null_counts(&self, leaf: usize, field: &Field) -> Option<ArrayRef>;

    /// Row counts, as a `UInt64Array`.
    fn row_counts(&self) -> Option<ArrayRef>;
}

/// Reads statistics for an expression from Parquet leaf statistics.
///
/// DataFusion provides a resolver for struct fields.
/// Other crates can add resolvers for their own functions, for example a
/// variant extension that reads the statistics of shredded `typed_value`
/// columns for `variant_get`. Register them with
/// [`ParquetSource::with_statistics_resolver`].
///
/// [`ParquetSource::with_statistics_resolver`]: crate::source::ParquetSource::with_statistics_resolver
pub trait ParquetStatisticsResolver: Debug + Send + Sync {
    /// Binds `expr` to the leaves of one file.
    ///
    /// Return `None` when this resolver has no statistics for `expr` in this
    /// file. Then the pruning predicate does not use `expr` as a statistics
    /// key, and it looks for keys in the children of `expr` instead. For
    /// example, a predicate on `TRY_CAST(e AS Int64)` can use the statistics
    /// of `e`.
    ///
    /// The pruning predicates of a file call `bind` for many nodes of each
    /// predicate, and again for each dynamic filter update. So a resolver
    /// must reject an expression that it does not handle cheaply, for
    /// example with one downcast.
    ///
    /// `EXPLAIN` has no file, so it binds against a Parquet schema that it
    /// builds from the Arrow file schema. A resolver for leaves that exist
    /// only in real files, such as shredded variant columns, binds nothing
    /// there. Its keys then do not show in the `EXPLAIN` pruning predicate,
    /// but the scan still uses them.
    fn bind(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<Arc<dyn BoundExprStatistics>>;
}

/// The statistics of one expression in one file, from
/// [`ParquetStatisticsResolver::bind`].
pub trait BoundExprStatistics: Debug + Send + Sync {
    /// Returns one value per container for `kind`.
    ///
    /// Bounds must be conservative: each non-null min (max) must be at most
    /// (at least) every non-null value of the expression in its container.
    /// Return null for a container whose statistics are unknown, and `None`
    /// when they are unknown for every container. Pruning keeps a container
    /// with unknown statistics.
    fn statistics(
        &self,
        kind: ExprStatisticsKind,
        statistics: &dyn ParquetLeafStatistics,
    ) -> Option<ArrayRef>;

    /// The Parquet leaf whose pages hold the values of the expression, if
    /// page index pruning can use these statistics.
    ///
    /// The page index prunes one leaf at a time, because the pages of
    /// different leaves do not line up. Return `None` (the default) when the
    /// statistics need more than one leaf.
    fn page_index_leaf(&self) -> Option<usize> {
        None
    }

    /// The Parquet leaf whose bloom filter can rule out values of the
    /// expression.
    ///
    /// Return a leaf only when every non-null value of the expression is
    /// stored in that leaf, with the leaf's physical type. Then a value
    /// missing from the leaf's bloom filter cannot be a value of the
    /// expression. The literal is hashed as its own type, so the expression
    /// must have the type of the leaf without any conversion.
    ///
    /// Return `None` (the default) otherwise. For example, a shredded
    /// variant may store some values in its binary `value` column, which a
    /// bloom filter on `typed_value` cannot see.
    fn bloom_filter_leaf(&self) -> Option<usize> {
        None
    }
}

/// The [`ParquetStatisticsResolver`]s of a Parquet scan.
///
/// The default holds [`GetFieldStatisticsResolver`]. For each file, the
/// scan binds them to the schema of that file.
#[derive(Debug, Clone)]
pub(crate) struct ParquetStatisticsResolvers {
    resolvers: Vec<Arc<dyn ParquetStatisticsResolver>>,
}

impl Default for ParquetStatisticsResolvers {
    fn default() -> Self {
        Self {
            resolvers: vec![Arc::new(GetFieldStatisticsResolver)],
        }
    }
}

impl ParquetStatisticsResolvers {
    /// Adds `resolver`. Resolvers are asked in the order they were added.
    pub(crate) fn with_resolver(
        mut self,
        resolver: Arc<dyn ParquetStatisticsResolver>,
    ) -> Self {
        self.resolvers.push(resolver);
        self
    }

    /// Returns `true` if resolvers beyond the built-in ones were added.
    #[cfg(feature = "proto")]
    pub(crate) fn has_custom_resolvers(&self) -> bool {
        self.resolvers.len() > Self::default().resolvers.len()
    }
}

type BoundStatistics = Arc<dyn BoundExprStatistics>;

/// [`ParquetStatisticsResolvers`] bound to the schema of one file.
///
/// This is the [`StatisticsExprMatcher`] of the pruning predicates for the
/// file. An expression is a statistics key when the first resolver binds
/// it. The predicate stores that binding in the key's [`StatsExpr`], and the
/// pruning statistics read the values from it (see [`bound_statistics`]).
/// So the key and its statistics always agree, and nothing looks the
/// expression up again.
#[derive(Debug)]
pub(crate) struct FileStatisticsResolvers {
    resolvers: ParquetStatisticsResolvers,
    arrow_schema: SchemaRef,
    parquet_schema: SchemaDescPtr,
}

impl FileStatisticsResolvers {
    /// Binds `resolvers` to a file with `arrow_schema` and `parquet_schema`.
    /// Expressions refer to the columns of `arrow_schema`.
    pub(crate) fn new(
        resolvers: ParquetStatisticsResolvers,
        arrow_schema: SchemaRef,
        parquet_schema: SchemaDescPtr,
    ) -> Self {
        Self {
            resolvers,
            arrow_schema,
            parquet_schema,
        }
    }
}

/// Two values are equal only when they are the same instance, because
/// resolvers have no equality of their own.
impl PartialEq for FileStatisticsResolvers {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl StatisticsExprMatcher for FileStatisticsResolvers {
    /// The binding of the first resolver that binds `expr`.
    fn bind(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<Arc<dyn Any + Send + Sync>> {
        let bound: BoundStatistics = self
            .resolvers
            .resolvers
            .iter()
            .find_map(|r| r.bind(expr, &self.arrow_schema, &self.parquet_schema))?;
        Some(Arc::new(bound))
    }
}

/// The binding that [`FileStatisticsResolvers`] created for `expr`, or
/// `None` when another matcher created it.
pub(crate) fn bound_statistics(expr: &StatsExpr) -> Option<&BoundStatistics> {
    expr.binding::<BoundStatistics>()
}

/// Returns the index of the Parquet leaf column at `path`, such as
/// `["s", "a"]` for the field `a` of the struct column `s`.
pub fn leaf_index_by_path(
    parquet_schema: &SchemaDescriptor,
    path: &[&str],
) -> Option<usize> {
    parquet_schema.columns().iter().position(|descr| {
        let parts = descr.path().parts();
        parts.len() == path.len() && parts.iter().zip(path).all(|(a, b)| a == b)
    })
}

/// Reads the statistics of `get_field(column, 'a', 'b', ...)` from the
/// Parquet leaf of that struct field.
///
/// Only struct paths are supported. A path through a map is a runtime key
/// lookup, and a field below a list has statistics per element, not per row.
#[derive(Debug, Default)]
pub(crate) struct GetFieldStatisticsResolver;

impl GetFieldStatisticsResolver {
    /// The root column and field path of `expr`, including chained calls
    /// such as `get_field(get_field(s, 'a'), 'b')`.
    fn path(expr: &Arc<dyn PhysicalExpr>) -> Option<(&Column, Vec<&str>)> {
        let func = ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())?;
        let (base, keys) = func.args().split_first()?;
        let (column, mut path) = match base.downcast_ref::<Column>() {
            Some(column) => (column, vec![]),
            None => Self::path(base)?,
        };
        for key in keys {
            match key.downcast_ref::<Literal>()?.value() {
                ScalarValue::Utf8(Some(name))
                | ScalarValue::LargeUtf8(Some(name))
                | ScalarValue::Utf8View(Some(name)) => path.push(name.as_str()),
                _ => return None,
            }
        }
        (!path.is_empty()).then_some((column, path))
    }

    /// The Parquet leaf index and Arrow field of `expr`.
    fn leaf<'a>(
        expr: &Arc<dyn PhysicalExpr>,
        arrow_schema: &'a Schema,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<(usize, &'a Field)> {
        let (column, path) = Self::path(expr)?;
        let mut field = arrow_schema.field_with_name(column.name()).ok()?;
        for name in &path {
            let DataType::Struct(fields) = field.data_type() else {
                return None;
            };
            field = fields.iter().find(|f| f.name() == name)?;
        }
        if field.data_type().is_nested() {
            return None;
        }
        let mut parts = vec![column.name()];
        parts.extend(path);
        let leaf = leaf_index_by_path(parquet_schema, &parts)?;
        (parquet_schema.column(leaf).max_rep_level() == 0).then_some((leaf, field))
    }
}

impl ParquetStatisticsResolver for GetFieldStatisticsResolver {
    fn bind(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<Arc<dyn BoundExprStatistics>> {
        let (leaf, field) = Self::leaf(expr, arrow_schema, parquet_schema)?;
        Some(Arc::new(StructFieldStatistics {
            leaf,
            field: field.clone(),
        }))
    }
}

/// The statistics of a struct field, read from its Parquet leaf.
#[derive(Debug)]
struct StructFieldStatistics {
    leaf: usize,
    field: Field,
}

impl BoundExprStatistics for StructFieldStatistics {
    fn statistics(
        &self,
        kind: ExprStatisticsKind,
        statistics: &dyn ParquetLeafStatistics,
    ) -> Option<ArrayRef> {
        match kind {
            ExprStatisticsKind::Min => statistics.leaf_mins(self.leaf, &self.field),
            ExprStatisticsKind::Max => statistics.leaf_maxes(self.leaf, &self.field),
            ExprStatisticsKind::NullCount => {
                statistics.leaf_null_counts(self.leaf, &self.field)
            }
        }
    }

    fn page_index_leaf(&self) -> Option<usize> {
        Some(self.leaf)
    }

    /// A struct field leaf holds every non-null value of the field.
    fn bloom_filter_leaf(&self) -> Option<usize> {
        Some(self.leaf)
    }
}
