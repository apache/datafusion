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

//! Implementation of [`array_col`]: attaching pre-materialized Arrow arrays
//! as [`DataFrame`] columns without executing the plan.

use super::DataFrame;
use crate::datasource::{TableProvider, provider_as_source, source_as_provider};
use crate::error::Result;
use crate::execution::context::{SessionState, TaskContext};
use crate::execution::session_state::SessionStateBuilder;
use crate::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableType};
use crate::physical_expr::EquivalenceProperties;
use crate::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PhysicalExpr,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, displayable,
    stream::RecordBatchStreamAdapter,
};

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_catalog::Session;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    ScalarValue, exec_err, internal_datafusion_err, internal_err, plan_err,
    project_schema,
};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    UNNAMED_TABLE, Volatility,
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

impl DataFrame {
    /// Lazy implementation behind [`array_col`]: wraps the current plan in an
    /// [`AddColumnProvider`] that zips `array` onto the rows at execution time.
    /// See [`array_col`] for the contract.
    pub(super) fn with_array_column(self, name: &str, array: ArrayRef) -> Result<DataFrame> {
        let (state, plan) = self.into_parts();
        // Consecutive `array_col` calls extend the existing provider instead of
        // nesting one sub-plan per column.
        let (input, mut columns) = match add_column_provider(&plan) {
            Some((input, columns)) => (input, columns),
            None => (plan, Vec::new()),
        };
        columns.push((name.to_string(), array));
        let provider = Arc::new(AddColumnProvider::try_new(input, columns)?);
        let plan =
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(provider), None)?
                .build()?;
        Ok(DataFrame::new(state, plan))
    }
}

/// Create an [`Expr`] that attaches an Arrow [`ArrayRef`] as a DataFrame column.
///
/// Pass the result to [`DataFrame::with_column`]. Unlike [`lit`], which repeats
/// one value on every row, the array is zipped onto the rows positionally:
/// element `i` of the array becomes the value of row `i`.
///
/// [`lit`]: datafusion_expr::lit
///
/// # Rules
///
/// * `array_col` is only valid as the expression passed to `with_column`,
///   optionally wrapped in an alias. Using it anywhere else — in `select`,
///   `filter`, or combined with other expressions — is rejected during
///   planning: by the builder where the expression's type is needed, otherwise
///   when the DataFrame is executed. It is never evaluated. To transform the
///   values, add the column first and then use a second `with_column` call on
///   the new column.
/// * If a column with that name already exists it is replaced in place, as
///   with any other `with_column` call; otherwise the column is appended.
/// * The new field is nullable only if the array may contain nulls
///   (`Array::is_nullable`).
/// * The array must have exactly as many elements as the DataFrame produces
///   rows; a mismatch is an error at execution time. Consecutive `array_col`
///   calls are planned as one operator, so their arrays must also agree with
///   each other, which is checked when the column is added.
/// * Rows are paired in the order the input DataFrame produces them when
///   executed as a single partition: in-memory sources (`from_columns`,
///   `read_batch`, `read_batches`) keep insertion order, and `filter`,
///   `select`, `sort` and `limit` preserve it. A multi-partition `MemTable`
///   is read partition by partition. Inputs without an inherent row order —
///   joins, aggregations, multi-file scans — should be sorted first. The
///   input is executed with a single partition; operations after
///   `with_column` are parallelized as usual.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use arrow::array::{ArrayRef, Int32Array, StringArray};
/// use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # use datafusion_common::assert_batches_sorted_eq;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let id: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
/// let name: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
/// let df = DataFrame::from_columns([("id", id), ("name", name)])?;
///
/// let extra: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
/// let df = df.with_column("extra", array_col(extra))?;
///
/// let expected = vec![
///     "+----+------+-------+",
///     "| id | name | extra |",
///     "+----+------+-------+",
///     "| 1  | foo  | 10    |",
///     "| 2  | bar  | 20    |",
///     "| 3  | baz  | 30    |",
///     "+----+------+-------+",
/// ];
/// # assert_batches_sorted_eq!(expected, &df.collect().await?);
/// # Ok(())
/// # }
/// ```
pub fn array_col(array: ArrayRef) -> Expr {
    ScalarUDF::new_from_impl(ArrayCol::new(array)).call(vec![])
}

/// Marker UDF used by [`array_col`]. [`DataFrame::with_column`] intercepts this
/// expression and zips the array onto existing rows. Evaluating it as a normal
/// scalar function is not supported.
#[derive(Debug)]
struct ArrayCol {
    array: ArrayRef,
    signature: Signature,
}

impl ArrayCol {
    fn new(array: ArrayRef) -> Self {
        Self {
            array,
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl PartialEq for ArrayCol {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.array, &other.array)
    }
}

impl Eq for ArrayCol {}

impl Hash for ArrayCol {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.array.data_type().hash(state);
        self.array.len().hash(state);
        Arc::as_ptr(&self.array).cast::<()>().hash(state);
    }
}

impl ScalarUDFImpl for ArrayCol {
    fn name(&self) -> &str {
        "array_col"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!(
            "array_col() can only be used as the expression passed to DataFrame::with_column. \
             To transform the values, add the column first and use a second with_column call"
        )
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("array_col() was not intercepted by DataFrame::with_column")
    }

    fn should_evaluate_const(&self, _args: &[&ScalarValue]) -> bool {
        false
    }
}

/// Extract the array from an [`array_col`] expression, unwrapping aliases.
pub(super) fn take_array_col(mut expr: &Expr) -> Option<ArrayRef> {
    while let Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    match expr {
        Expr::ScalarFunction(func) if func.args.is_empty() => func
            .func
            .inner()
            .downcast_ref::<ArrayCol>()
            .map(|array_col| Arc::clone(&array_col.array)),
        _ => None,
    }
}

/// When `plan` is a bare scan produced by [`DataFrame::with_array_column`],
/// returns that provider's input plan and `(name, array)` columns so a further
/// array column can be added to the same provider instead of nesting.
fn add_column_provider(
    plan: &LogicalPlan,
) -> Option<(LogicalPlan, Vec<(String, ArrayRef)>)> {
    let LogicalPlan::TableScan(scan) = plan else {
        return None;
    };
    if scan.projection.is_some() || !scan.filters.is_empty() || scan.fetch.is_some() {
        return None;
    }
    let provider = source_as_provider(&scan.source).ok()?;
    let provider = provider.downcast_ref::<AddColumnProvider>()?;
    Some((provider.input.clone(), provider.added_columns()))
}

/// A column attached by [`array_col`] and where it goes in the output.
#[derive(Debug, Clone)]
struct AddedColumn {
    name: String,
    array: ArrayRef,
    /// Index of the column in the output: the index of the replaced input
    /// column, or a new index past the input's fields when appended.
    position: usize,
}

/// [`TableProvider`] that wraps a [`LogicalPlan`] and attaches one or more
/// pre-materialized arrays as columns when it is executed.
#[derive(Debug)]
struct AddColumnProvider {
    input: LogicalPlan,
    /// Sorted by `position`, so appended columns are in output order.
    columns: Vec<AddedColumn>,
    schema: SchemaRef,
}

impl AddColumnProvider {
    fn try_new(input: LogicalPlan, columns: Vec<(String, ArrayRef)>) -> Result<Self> {
        if columns.is_empty() {
            return internal_err!("AddColumnProvider requires at least one column");
        }

        let mut fields: Vec<Field> = input
            .schema()
            .as_arrow()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut added: Vec<AddedColumn> = Vec::with_capacity(columns.len());
        let mut expected_len: Option<(String, usize)> = None;

        for (name, array) in columns {
            // Every array is paired with the same rows, so all of them must
            // have the same length. The input's row count is only known at
            // execution time and is checked there.
            match &expected_len {
                Some((first_name, len)) if *len != array.len() => {
                    return plan_err!(
                        "Column '{name}' has {} rows but column '{first_name}' has {len}: \
                         all arrays added to a DataFrame must have the same length",
                        array.len()
                    );
                }
                Some(_) => {}
                None => expected_len = Some((name.clone(), array.len())),
            }

            let new_field =
                Field::new(&name, array.data_type().clone(), array.is_nullable());

            // Same contract as `with_column` with an expression: an existing
            // column is replaced in place, a new name is appended. A column
            // added earlier by this provider is replaced the same way.
            let matches: Vec<usize> = fields
                .iter()
                .enumerate()
                .filter(|(_, f)| f.name() == &name)
                .map(|(i, _)| i)
                .collect();
            let position = match matches.as_slice() {
                [] => {
                    fields.push(new_field);
                    fields.len() - 1
                }
                [i] => {
                    fields[*i] = new_field;
                    *i
                }
                _ => {
                    return plan_err!(
                        "Column '{name}' is ambiguous: {} input columns have that name",
                        matches.len()
                    );
                }
            };
            added.retain(|c| c.position != position);
            added.push(AddedColumn {
                name,
                array,
                position,
            });
        }
        added.sort_by_key(|c| c.position);

        Ok(Self {
            input,
            columns: added,
            schema: Arc::new(Schema::new(fields)),
        })
    }

    /// The columns as `(name, array)` pairs, for building an extended provider.
    fn added_columns(&self) -> Vec<(String, ArrayRef)> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), Arc::clone(&c.array)))
            .collect()
    }
}

#[async_trait]
impl TableProvider for AddColumnProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Positional pairing is only defined on a stream that has not been
        // repartitioned: row `i` of the input must meet element `i` of the
        // arrays. Plan the input as a single-partition sub-plan so the
        // optimizer does not split the source or insert `RepartitionExec`
        // below us.
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                internal_datafusion_err!("AddColumnProvider requires a SessionState")
            })?;
        let config = state.config().clone().with_target_partitions(1);
        let single_partition_state =
            SessionStateBuilder::new_from_existing(state.clone())
                .with_config(config)
                .build();
        let input = single_partition_state
            .create_physical_plan(&self.input)
            .await?;
        Ok(Arc::new(AddColumnExec::new(
            input,
            self.columns.clone(),
            Arc::clone(&self.schema),
            projection,
        )?))
    }
}

/// Attaches pre-materialized arrays as columns, pairing array element `i`
/// with the `i`-th row produced by `input`.
///
/// Positional pairing is only defined on a stream that has not been
/// repartitioned. `input` is therefore a fully planned, single-partition
/// sub-plan (see [`AddColumnProvider::scan`]) that is deliberately *not*
/// exposed through [`ExecutionPlan::children`]: the physical optimizer would
/// otherwise repartition it to increase parallelism, and the merged stream
/// would no longer be in input order. Any partitions the input still has are
/// inherent to its source (for example a multi-partition `MemTable`) and are
/// read one after another, in partition order.
#[derive(Debug)]
struct AddColumnExec {
    input: Arc<dyn ExecutionPlan>,
    /// Sorted by position; see [`AddColumnProvider`].
    columns: Arc<[AddedColumn]>,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    cache: Arc<PlanProperties>,
}

impl AddColumnExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        columns: Vec<AddedColumn>,
        full_schema: SchemaRef,
        projection: Option<&[usize]>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&full_schema, projection)?;
        Ok(Self {
            cache: Arc::new(Self::compute_properties(projected_schema, input.as_ref())),
            input,
            columns: columns.into(),
            full_schema,
            projection: projection.map(|p| p.to_vec()),
        })
    }

    fn compute_properties(
        schema: SchemaRef,
        input: &dyn ExecutionPlan,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }

    fn input_partitions(&self) -> usize {
        self.input.output_partitioning().partition_count()
    }

    /// Column names for display, marking the ones that replace an input column.
    fn column_names(&self) -> String {
        let input_fields = self.input.schema().fields().len();
        self.columns
            .iter()
            .map(|c| {
                if c.position < input_fields {
                    format!("{} (replace)", c.name)
                } else {
                    c.name.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl DisplayAs for AddColumnExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "AddColumnExec: {}, input_partitions={}",
                    self.column_names(),
                    self.input_partitions()
                )?;
                // The input is not a child, so the plan printer will not
                // descend into it. Render it here so EXPLAIN shows what runs.
                let verbose = matches!(t, DisplayFormatType::Verbose);
                let input = displayable(self.input.as_ref()).indent(verbose).to_string();
                for line in input.lines() {
                    write!(f, "\n  {line}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "columns={}", self.column_names())?;
                write!(f, "input_partitions={}", self.input_partitions())
            }
        }
    }
}

impl ExecutionPlan for AddColumnExec {
    fn name(&self) -> &'static str {
        "AddColumnExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    /// The input is intentionally not reported as a child; see the type docs.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("AddColumnExec has no children");
        }
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("AddColumnExec only produces partition 0");
        }

        // Read the input partitions one after another, in partition order.
        // Reading them concurrently (e.g. through `CoalescePartitionsExec`)
        // would interleave batches in arrival order and pair rows with the
        // wrong array elements.
        let input = Arc::clone(&self.input);
        let stream = futures::stream::iter(0..self.input_partitions())
            .map(move |p| input.execute(p, Arc::clone(&context)))
            .try_flatten()
            .boxed();

        let columns = Arc::clone(&self.columns);
        let full_schema = Arc::clone(&self.full_schema);
        let projection = self.projection.clone();
        // All arrays have the same length; see `AddColumnProvider::try_new`.
        let expected_rows = columns[0].array.len();
        let out_schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            out_schema,
            futures::stream::try_unfold((stream, 0usize), move |(mut stream, offset)| {
                let columns = Arc::clone(&columns);
                let full_schema = Arc::clone(&full_schema);
                let projection = projection.clone();

                async move {
                    match stream.next().await {
                        Some(Ok(batch)) => {
                            let n = batch.num_rows();
                            if offset + n > expected_rows {
                                return exec_err!(
                                    "Added columns have {expected_rows} rows, \
                                         dataframe produced at least {}",
                                    offset + n
                                );
                            }
                            let mut out = batch.columns().to_vec();
                            // `columns` is sorted by position, so appended
                            // columns are pushed in output order.
                            for added in columns.iter() {
                                let column = added.array.slice(offset, n);
                                if added.position < out.len() {
                                    out[added.position] = column;
                                } else {
                                    out.push(column);
                                }
                            }
                            let full = RecordBatch::try_new(full_schema, out)?;
                            let batch = match &projection {
                                Some(indices) => full.project(indices)?,
                                None => full,
                            };
                            Ok(Some((batch, (stream, offset + n))))
                        }
                        Some(Err(e)) => Err(e),
                        None if offset != expected_rows => exec_err!(
                            "Added columns have {expected_rows} rows, \
                                 dataframe produced {offset}"
                        ),
                        None => Ok(None),
                    }
                }
            }),
        )))
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}
