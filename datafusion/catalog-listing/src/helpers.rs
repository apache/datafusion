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

//! Helper functions for the table implementation

use std::mem;
use std::sync::Arc;

use datafusion_catalog::Session;
use datafusion_common::{assert_or_internal_err, HashMap, Result, ScalarValue};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::PartitionedFile;
use datafusion_expr::{lit, utils, BinaryExpr, Operator};

use arrow::{
    array::AsArray,
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use datafusion_expr::execution_props::ExecutionProps;
use futures::stream::FuturesUnordered;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use log::{debug, trace};

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema};
use datafusion_expr::{Expr, Volatility};
use datafusion_physical_expr::create_physical_expr;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

/// Check whether the given expression can be resolved using only the columns `col_names`.
/// This means that if this function returns true:
/// - the table provider can filter the table partition values with this expression
/// - the expression can be marked as `TableProviderFilterPushDown::Exact` once this filtering
///   was performed
pub fn expr_applicable_for_cols(col_names: &[&str], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.apply(|expr| match expr {
        Expr::Column(Column { ref name, .. }) => {
            is_applicable &= col_names.contains(&name.as_str());
            if is_applicable {
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        }
        Expr::Literal(_, _)
        | Expr::Alias(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::ScalarVariable(_, _)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::BinaryExpr(_)
        | Expr::Between(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::InList(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::GroupingSet(_)
        | Expr::Case(_) => Ok(TreeNodeRecursion::Continue),

        Expr::ScalarFunction(scalar_function) => {
            match scalar_function.func.signature().volatility {
                Volatility::Immutable => Ok(TreeNodeRecursion::Continue),
                // TODO: Stable functions could be `applicable`, but that would require access to the context
                Volatility::Stable | Volatility::Volatile => {
                    is_applicable = false;
                    Ok(TreeNodeRecursion::Stop)
                }
            }
        }

        // TODO other expressions are not handled yet:
        // - AGGREGATE and WINDOW should not end up in filter conditions, except maybe in some edge cases
        // - Can `Wildcard` be considered as a `Literal`?
        // - ScalarVariable could be `applicable`, but that would require access to the context
        // TODO: remove the next line after `Expr::Wildcard` is removed
        #[expect(deprecated)]
        Expr::AggregateFunction { .. }
        | Expr::WindowFunction { .. }
        | Expr::Wildcard { .. }
        | Expr::Unnest { .. }
        | Expr::Placeholder(_) => {
            is_applicable = false;
            Ok(TreeNodeRecursion::Stop)
        }
    })
    .unwrap();
    is_applicable
}

/// The maximum number of concurrent listing requests
const CONCURRENCY_LIMIT: usize = 100;

/// Partition the list of files into `n` groups
#[deprecated(since = "47.0.0", note = "use `FileGroup::split_files` instead")]
pub fn split_files(
    mut partitioned_files: Vec<PartitionedFile>,
    n: usize,
) -> Vec<Vec<PartitionedFile>> {
    if partitioned_files.is_empty() {
        return vec![];
    }

    // ObjectStore::list does not guarantee any consistent order and for some
    // implementations such as LocalFileSystem, it may be inconsistent. Thus
    // Sort files by path to ensure consistent plans when run more than once.
    partitioned_files.sort_by(|a, b| a.path().cmp(b.path()));

    // effectively this is div with rounding up instead of truncating
    let chunk_size = partitioned_files.len().div_ceil(n);
    let mut chunks = Vec::with_capacity(n);
    let mut current_chunk = Vec::with_capacity(chunk_size);
    for file in partitioned_files.drain(..) {
        current_chunk.push(file);
        if current_chunk.len() == chunk_size {
            let full_chunk =
                mem::replace(&mut current_chunk, Vec::with_capacity(chunk_size));
            chunks.push(full_chunk);
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk)
    }

    chunks
}

#[derive(Debug)]
pub struct Partition {
    /// The path to the partition, including the table prefix
    path: Path,
    /// How many path segments below the table prefix `path` contains
    /// or equivalently the number of partition values in `path`
    depth: usize,
    /// The files contained as direct children of this `Partition` if known
    files: Option<Vec<ObjectMeta>>,
}

impl Partition {
    /// List the direct children of this partition updating `self.files` with
    /// any child files, and returning a list of child "directories"
    async fn list(mut self, store: &dyn ObjectStore) -> Result<(Self, Vec<Path>)> {
        trace!("Listing partition {}", self.path);
        let prefix = Some(&self.path).filter(|p| !p.as_ref().is_empty());
        let result = store.list_with_delimiter(prefix).await?;
        self.files = Some(
            result
                .objects
                .into_iter()
                .filter(|object_meta| object_meta.size > 0)
                .collect(),
        );
        Ok((self, result.common_prefixes))
    }
}

/// Returns a recursive list of the partitions in `table_path` up to `max_depth`
pub async fn list_partitions(
    store: &dyn ObjectStore,
    table_path: &ListingTableUrl,
    max_depth: usize,
    partition_prefix: Option<Path>,
) -> Result<Vec<Partition>> {
    let partition = Partition {
        path: match partition_prefix {
            Some(prefix) => Path::from_iter(
                Path::from(table_path.prefix().as_ref())
                    .parts()
                    .chain(Path::from(prefix.as_ref()).parts()),
            ),
            None => table_path.prefix().clone(),
        },
        depth: 0,
        files: None,
    };

    let mut out = Vec::with_capacity(64);

    let mut pending = vec![];
    let mut futures = FuturesUnordered::new();
    futures.push(partition.list(store));

    while let Some((partition, paths)) = futures.next().await.transpose()? {
        // If pending contains a future it implies prior to this iteration
        // `futures.len == CONCURRENCY_LIMIT`. We can therefore add a single
        // future from `pending` to the working set
        if let Some(next) = pending.pop() {
            futures.push(next)
        }

        let depth = partition.depth;
        out.push(partition);
        for path in paths {
            let child = Partition {
                path,
                depth: depth + 1,
                files: None,
            };
            match depth < max_depth {
                true => match futures.len() < CONCURRENCY_LIMIT {
                    true => futures.push(child.list(store)),
                    false => pending.push(child.list(store)),
                },
                false => out.push(child),
            }
        }
    }
    Ok(out)
}

#[derive(Debug)]
enum PartitionValue {
    Single(String),
    Multi,
}

fn populate_partition_values<'a>(
    partition_values: &mut HashMap<&'a str, PartitionValue>,
    filter: &'a Expr,
) {
    if let Expr::BinaryExpr(BinaryExpr {
        ref left,
        op,
        ref right,
    }) = filter
    {
        match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(Column { ref name, .. }), Expr::Literal(val, _))
                | (Expr::Literal(val, _), Expr::Column(Column { ref name, .. })) => {
                    if partition_values
                        .insert(name, PartitionValue::Single(val.to_string()))
                        .is_some()
                    {
                        partition_values.insert(name, PartitionValue::Multi);
                    }
                }
                _ => {}
            },
            Operator::And => {
                populate_partition_values(partition_values, left);
                populate_partition_values(partition_values, right);
            }
            _ => {}
        }
    }
}

pub fn evaluate_partition_prefix<'a>(
    partition_cols: &'a [(String, DataType)],
    filters: &'a [Expr],
) -> Option<Path> {
    let mut partition_values = HashMap::new();
    for filter in filters {
        populate_partition_values(&mut partition_values, filter);
    }

    if partition_values.is_empty() {
        return None;
    }

    let mut parts = vec![];
    for (p, _) in partition_cols {
        match partition_values.get(p.as_str()) {
            Some(PartitionValue::Single(val)) => {
                // if a partition only has a single literal value, then it can be added to the
                // prefix
                parts.push(format!("{p}={val}"));
            }
            _ => {
                // break on the first unconstrainted partition to create a common prefix
                // for all covered partitions.
                break;
            }
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(Path::from_iter(parts))
    }
}

fn filter_partitions(
    pf: PartitionedFile,
    filters: &[Expr],
    df_schema: &DFSchema,
) -> Result<Option<PartitionedFile>> {
    if pf.partition_values.is_empty() && !filters.is_empty() {
        return Ok(None);
    } else if filters.is_empty() {
        return Ok(Some(pf));
    }

    let arrays = pf
        .partition_values
        .iter()
        .map(|v| v.to_array())
        .collect::<Result<_, _>>()?;

    let batch = RecordBatch::try_new(Arc::clone(df_schema.inner()), arrays)?;

    let filter = utils::conjunction(filters.iter().cloned()).unwrap_or_else(|| lit(true));
    let props = ExecutionProps::new();
    let expr = create_physical_expr(&filter, df_schema, &props)?;

    // Since we're only operating on a single file, our batch and resulting "array" holds only one
    // value indicating if the input file matches the provided filters
    let matches = expr.evaluate(&batch)?.into_array(1)?;
    if matches.as_boolean().value(0) {
        return Ok(Some(pf));
    }

    Ok(None)
}

fn try_into_partitioned_file(
    object_meta: ObjectMeta,
    partition_cols: &[(String, DataType)],
    table_path: &ListingTableUrl,
) -> Result<PartitionedFile> {
    let cols = partition_cols.iter().map(|(name, _)| name.as_str());
    let parsed = parse_partitions_for_path(table_path, &object_meta.location, cols);

    let partition_values = parsed
        .into_iter()
        .flatten()
        .zip(partition_cols)
        .map(|(parsed, (_, datatype))| {
            ScalarValue::try_from_string(parsed.to_string(), datatype)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut pf: PartitionedFile = object_meta.into();
    pf.partition_values = partition_values;

    Ok(pf)
}

/// Discover the partitions on the given path and prune out files
/// that belong to irrelevant partitions using `filters` expressions.
/// `filters` should only contain expressions that can be evaluated
/// using only the partition columns.
pub async fn pruned_partition_list<'a>(
    ctx: &'a dyn Session,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    file_extension: &'a str,
    partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    let prefix = if !partition_cols.is_empty() {
        evaluate_partition_prefix(partition_cols, filters)
    } else {
        None
    };

    let objects = table_path
        .list_prefixed_files(ctx, store, prefix, file_extension)
        .await?
        .try_filter(|object_meta| futures::future::ready(object_meta.size > 0));

    if partition_cols.is_empty() {
        assert_or_internal_err!(
            filters.is_empty(),
            "Got partition filters for unpartitioned table {}",
            table_path
        );

        // if no partition col => simply list all the files
        Ok(objects.map_ok(|object_meta| object_meta.into()).boxed())
    } else {
        let df_schema = DFSchema::from_unqualified_fields(
            partition_cols
                .iter()
                .map(|(n, d)| Field::new(n, d.clone(), true))
                .collect(),
            Default::default(),
        )?;

        Ok(objects
            .map_ok(|object_meta| {
                try_into_partitioned_file(object_meta, partition_cols, table_path)
            })
            .try_filter_map(move |pf| {
                futures::future::ready(
                    pf.and_then(|pf| filter_partitions(pf, filters, &df_schema)),
                )
            })
            .boxed())
    }
}

/// Extract the partition values for the given `file_path` (in the given `table_path`)
/// associated to the partitions defined by `table_partition_cols`
pub fn parse_partitions_for_path<'a, I>(
    table_path: &ListingTableUrl,
    file_path: &'a Path,
    table_partition_cols: I,
) -> Option<Vec<&'a str>>
where
    I: IntoIterator<Item = &'a str>,
{
    let subpath = table_path.strip_prefix(file_path)?;

    let mut part_values = vec![];
    for (part, expected_partition) in subpath.zip(table_partition_cols) {
        match part.split_once('=') {
            Some((name, val)) if name == expected_partition => part_values.push(val),
            _ => {
                debug!(
                    "Ignoring file: file_path='{file_path}', table_path='{table_path}', part='{part}', partition_col='{expected_partition}'",
                );
                return None;
            }
        }
    }
    Some(part_values)
}
/// Describe a partition as a (path, depth, files) tuple for easier assertions
pub fn describe_partition(partition: &Partition) -> (&str, usize, Vec<&str>) {
    (
        partition.path.as_ref(),
        partition.depth,
        partition
            .files
            .as_ref()
            .map(|f| f.iter().map(|f| f.location.filename().unwrap()).collect())
            .unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use datafusion_datasource::file_groups::FileGroup;
    use std::ops::Not;

    use super::*;
    use datafusion_expr::{case, col, lit, Expr};

    #[test]
    fn test_split_files() {
        let new_partitioned_file = |path: &str| PartitionedFile::new(path.to_owned(), 10);
        let files = FileGroup::new(vec![
            new_partitioned_file("a"),
            new_partitioned_file("b"),
            new_partitioned_file("c"),
            new_partitioned_file("d"),
            new_partitioned_file("e"),
        ]);

        let chunks = files.clone().split_files(1);
        assert_eq!(1, chunks.len());
        assert_eq!(5, chunks[0].len());

        let chunks = files.clone().split_files(2);
        assert_eq!(2, chunks.len());
        assert_eq!(3, chunks[0].len());
        assert_eq!(2, chunks[1].len());

        let chunks = files.clone().split_files(5);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let chunks = files.clone().split_files(123);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let empty_group = FileGroup::default();
        let chunks = empty_group.split_files(2);
        assert_eq!(0, chunks.len());
    }

    #[test]
    fn test_parse_partitions_for_path() {
        assert_eq!(
            Some(vec![]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                vec![]
            )
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/othertable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                vec![]
            )
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                vec!["mypartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/file.csv"),
                vec!["mypartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable/").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/file.csv"),
                vec!["mypartition"]
            )
        );
        // Only hive style partitioning supported for now:
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/v1/file.csv"),
                vec!["mypartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1", "v2"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/otherpartition=v2/file.csv"),
                vec!["mypartition", "otherpartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/otherpartition=v2/file.csv"),
                vec!["mypartition"]
            )
        );
    }

    #[test]
    fn test_expr_applicable_for_cols() {
        assert!(expr_applicable_for_cols(
            &["c1"],
            &Expr::eq(col("c1"), lit("value"))
        ));
        assert!(!expr_applicable_for_cols(
            &["c1"],
            &Expr::eq(col("c2"), lit("value"))
        ));
        assert!(!expr_applicable_for_cols(
            &["c1"],
            &Expr::eq(col("c1"), col("c2"))
        ));
        assert!(expr_applicable_for_cols(
            &["c1", "c2"],
            &Expr::eq(col("c1"), col("c2"))
        ));
        assert!(expr_applicable_for_cols(
            &["c1", "c2"],
            &(Expr::eq(col("c1"), col("c2").alias("c2_alias"))).not()
        ));
        assert!(expr_applicable_for_cols(
            &["c1", "c2"],
            &(case(col("c1"))
                .when(lit("v1"), lit(true))
                .otherwise(lit(false))
                .expect("valid case expr"))
        ));
        // static expression not relevant in this context but we
        // test it as an edge case anyway in case we want to generalize
        // this helper function
        assert!(expr_applicable_for_cols(&[], &lit(true)));
    }

    #[test]
    fn test_evaluate_partition_prefix() {
        let partitions = &[
            ("a".to_string(), DataType::Utf8),
            ("b".to_string(), DataType::Int16),
            ("c".to_string(), DataType::Boolean),
        ];

        assert_eq!(
            evaluate_partition_prefix(partitions, &[col("a").eq(lit("foo"))]),
            Some(Path::from("a=foo")),
        );

        assert_eq!(
            evaluate_partition_prefix(partitions, &[lit("foo").eq(col("a"))]),
            Some(Path::from("a=foo")),
        );

        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                &[col("a").eq(lit("foo")).and(col("b").eq(lit("bar")))],
            ),
            Some(Path::from("a=foo/b=bar")),
        );

        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                // list of filters should be evaluated as AND
                &[col("a").eq(lit("foo")), col("b").eq(lit("bar")),],
            ),
            Some(Path::from("a=foo/b=bar")),
        );

        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                &[col("a")
                    .eq(lit("foo"))
                    .and(col("b").eq(lit("1")))
                    .and(col("c").eq(lit("true")))],
            ),
            Some(Path::from("a=foo/b=1/c=true")),
        );

        // no prefix when filter is empty
        assert_eq!(evaluate_partition_prefix(partitions, &[]), None);

        // b=foo results in no prefix because a is not restricted
        assert_eq!(
            evaluate_partition_prefix(partitions, &[Expr::eq(col("b"), lit("foo"))]),
            None,
        );

        // a=foo and c=baz only results in preifx a=foo because b is not restricted
        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                &[col("a").eq(lit("foo")).and(col("c").eq(lit("baz")))],
            ),
            Some(Path::from("a=foo")),
        );

        // partition with multiple values results in no prefix
        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                &[Expr::and(col("a").eq(lit("foo")), col("a").eq(lit("bar")))],
            ),
            None,
        );

        // no prefix because partition a is not restricted to a single literal
        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                &[Expr::or(col("a").eq(lit("foo")), col("a").eq(lit("bar")))],
            ),
            None,
        );
        assert_eq!(
            evaluate_partition_prefix(partitions, &[col("b").lt(lit(5))],),
            None,
        );
    }

    #[test]
    fn test_evaluate_date_partition_prefix() {
        let partitions = &[("a".to_string(), DataType::Date32)];
        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                &[col("a").eq(Expr::Literal(ScalarValue::Date32(Some(3)), None))],
            ),
            Some(Path::from("a=1970-01-04")),
        );

        let partitions = &[("a".to_string(), DataType::Date64)];
        assert_eq!(
            evaluate_partition_prefix(
                partitions,
                &[col("a").eq(Expr::Literal(
                    ScalarValue::Date64(Some(4 * 24 * 60 * 60 * 1000)),
                    None
                )),],
            ),
            Some(Path::from("a=1970-01-05")),
        );
    }
}
