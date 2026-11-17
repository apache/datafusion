// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Column pruning for [`BoundedWindowAggExec`](super::BoundedWindowAggExec).

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::RecordBatchOptions;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, internal_err};
use datafusion_expr::WindowFrame;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{Column, LambdaExpr, LambdaVariable};
use datafusion_physical_expr::window::{
    PlainAggregateWindowExpr, SlidingAggregateWindowExpr, StandardWindowExpr, WindowExpr,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

use crate::windows::WindowUDFExpr;

/// The input columns that the window expressions of a
/// [`BoundedWindowAggExec`](super::BoundedWindowAggExec) read, and the window
/// expressions rewritten to read them from a batch that holds only those
/// columns.
///
/// The operator passes every input column through to its output, but only the
/// columns the expressions evaluate are needed to split the input by partition
/// and to buffer each partition. Buffering just those makes the cost of both
/// independent of the width of the input.
///
/// A window expression of an unknown kind, or one that cannot be rewritten
/// faithfully, keeps every column.
pub(super) struct PrunedInput {
    /// The input index of each kept column, ascending. `None` keeps every column.
    columns: Option<Vec<usize>>,
    /// The schema of the batches that [`Self::project`] returns.
    schema: SchemaRef,
    /// The window expressions to evaluate against those batches.
    window_expr: Vec<Arc<dyn WindowExpr>>,
}

impl PrunedInput {
    pub(super) fn new(
        window_expr: &[Arc<dyn WindowExpr>],
        input_schema: &SchemaRef,
    ) -> Arc<Self> {
        Arc::new(
            Self::prune(window_expr, input_schema).unwrap_or_else(|| Self {
                columns: None,
                schema: Arc::clone(input_schema),
                window_expr: window_expr.to_vec(),
            }),
        )
    }

    /// `batch` restricted to the kept columns, typed with the schema the plan
    /// declares for them rather than the batch's own, which may differ in
    /// nullability (<https://github.com/apache/datafusion/issues/9320>).
    pub(super) fn project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = match &self.columns {
            Some(indices) => indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect(),
            None => batch.columns().to_vec(),
        };
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            columns,
            &options,
        )?)
    }

    /// The window expressions to evaluate against the batches that
    /// [`Self::project`] returns.
    pub(super) fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    fn prune(
        window_expr: &[Arc<dyn WindowExpr>],
        input_schema: &SchemaRef,
    ) -> Option<Self> {
        let parts = window_expr
            .iter()
            .map(|expr| Parts::new(expr.as_ref()))
            .collect::<Option<Vec<_>>>()?;

        let mut used = BTreeSet::new();
        for expr in parts.iter().flat_map(Parts::expressions) {
            if !collect_column_indices(&expr, &mut used) {
                return None;
            }
        }
        let n_columns = input_schema.fields().len();
        if used.len() == n_columns || used.last().is_some_and(|&i| i >= n_columns) {
            return None;
        }

        let columns: Vec<usize> = used.into_iter().collect();
        let schema = Arc::new(input_schema.project(&columns).ok()?);
        let remap = Remap::new(input_schema, &columns, &schema);
        let window_expr = parts
            .iter()
            .map(|parts| {
                let rewritten = parts.rewrite(&remap)?;
                if rewritten.field()? != parts.expr.field()? {
                    return internal_err!("the rewritten expression has another field");
                }
                Ok(rewritten)
            })
            .collect::<Result<Vec<_>>>()
            .ok()?;
        Some(Self {
            columns: Some(columns),
            schema,
            window_expr,
        })
    }
}

/// Adds the index of every column `expr` reads to `used`. Returns `false` if
/// `expr` holds a lambda, whose column and variable indices share one space
/// that this rewrite does not renumber.
fn collect_column_indices(
    expr: &Arc<dyn PhysicalExpr>,
    used: &mut BTreeSet<usize>,
) -> bool {
    let mut supported = true;
    expr.apply(|expr| {
        if expr.downcast_ref::<LambdaExpr>().is_some()
            || expr.downcast_ref::<LambdaVariable>().is_some()
        {
            supported = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Some(column) = expr.downcast_ref::<Column>() {
            used.insert(column.index());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("the visitor does not fail");
    supported
}

/// Maps the column indices of the input to those of the kept columns.
struct Remap {
    /// The new index of each input column, `None` for the dropped ones.
    new_index: Vec<Option<usize>>,
    input_schema: SchemaRef,
    schema: SchemaRef,
}

impl Remap {
    fn new(input_schema: &SchemaRef, kept: &[usize], schema: &SchemaRef) -> Self {
        let mut new_index = vec![None; input_schema.fields().len()];
        for (new, &old) in kept.iter().enumerate() {
            new_index[old] = Some(new);
        }
        Self {
            new_index,
            input_schema: Arc::clone(input_schema),
            schema: Arc::clone(schema),
        }
    }

    /// `expr` reading the kept columns by their new indices. Errors if the
    /// result does not have the type and nullability `expr` has on the input.
    fn expr(&self, expr: &Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewritten = Arc::clone(expr)
            .transform_up(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                match self.new_index.get(column.index()) {
                    Some(Some(new)) => Ok(Transformed::yes(Arc::new(Column::new(
                        column.name(),
                        *new,
                    )) as _)),
                    _ => internal_err!("column {column} is not kept"),
                }
            })?
            .data;
        let before = expr.return_field(&self.input_schema)?;
        let after = rewritten.return_field(&self.schema)?;
        if before.data_type() != after.data_type()
            || before.is_nullable() != after.is_nullable()
        {
            return internal_err!("rewriting {expr} changed its type");
        }
        Ok(rewritten)
    }

    fn exprs(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        exprs.iter().map(|expr| self.expr(expr)).collect()
    }

    fn sort_exprs(&self, exprs: &[PhysicalSortExpr]) -> Result<Vec<PhysicalSortExpr>> {
        exprs
            .iter()
            .map(|sort| Ok(PhysicalSortExpr::new(self.expr(&sort.expr)?, sort.options)))
            .collect()
    }

    fn aggregate(
        &self,
        aggregate: &AggregateFunctionExpr,
    ) -> Result<AggregateFunctionExpr> {
        AggregateExprBuilder::new(
            Arc::new(aggregate.fun().clone()),
            self.exprs(&aggregate.expressions())?,
        )
        .alias(aggregate.name())
        .schema(Arc::clone(&self.schema))
        .order_by(self.sort_exprs(aggregate.order_bys())?)
        .with_ignore_nulls(aggregate.ignore_nulls())
        .with_distinct(aggregate.is_distinct())
        .with_reversed(aggregate.is_reversed())
        .build()
    }
}

/// The parts of a built-in window expression.
struct Parts<'a> {
    expr: &'a dyn WindowExpr,
    function: Function<'a>,
}

enum Function<'a> {
    /// An aggregate over a frame that only grows.
    Plain {
        aggregate: &'a AggregateFunctionExpr,
        filter: Option<&'a Arc<dyn PhysicalExpr>>,
    },
    /// An aggregate over a frame that moves.
    Sliding {
        aggregate: &'a AggregateFunctionExpr,
        filter: Option<&'a Arc<dyn PhysicalExpr>>,
    },
    Udwf(&'a WindowUDFExpr),
}

impl<'a> Parts<'a> {
    /// `None` if `expr` is not one of the built-in window expressions.
    fn new(expr: &'a dyn WindowExpr) -> Option<Self> {
        let any = expr.as_any();
        let function = if let Some(plain) = any.downcast_ref::<PlainAggregateWindowExpr>()
        {
            Function::Plain {
                aggregate: plain.get_aggregate_expr(),
                filter: plain.get_filter_expr(),
            }
        } else if let Some(sliding) = any.downcast_ref::<SlidingAggregateWindowExpr>() {
            Function::Sliding {
                aggregate: sliding.get_aggregate_expr(),
                filter: sliding.get_filter_expr(),
            }
        } else {
            let standard = any.downcast_ref::<StandardWindowExpr>()?;
            Function::Udwf(
                standard
                    .get_standard_func_expr()
                    .as_any()
                    .downcast_ref::<WindowUDFExpr>()?,
            )
        };
        Some(Self { expr, function })
    }

    /// Every expression the window expression evaluates against input rows.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut exprs = self.expr.partition_by().to_vec();
        exprs.extend(
            self.expr
                .order_by()
                .iter()
                .map(|sort| Arc::clone(&sort.expr)),
        );
        match &self.function {
            Function::Plain { aggregate, filter }
            | Function::Sliding { aggregate, filter } => {
                exprs.extend(aggregate.expressions());
                exprs.extend(
                    aggregate
                        .order_bys()
                        .iter()
                        .map(|sort| Arc::clone(&sort.expr)),
                );
                exprs.extend(filter.map(Arc::clone));
            }
            Function::Udwf(udwf) => exprs.extend(udwf.args().iter().cloned()),
        }
        exprs
    }

    fn rewrite(&self, remap: &Remap) -> Result<Arc<dyn WindowExpr>> {
        let partition_by = remap.exprs(self.expr.partition_by())?;
        let order_by = remap.sort_exprs(self.expr.order_by())?;
        let frame: Arc<WindowFrame> = Arc::clone(self.expr.get_window_frame());
        Ok(match &self.function {
            Function::Plain { aggregate, filter } => {
                Arc::new(PlainAggregateWindowExpr::new(
                    Arc::new(remap.aggregate(aggregate)?),
                    &partition_by,
                    &order_by,
                    frame,
                    filter.map(|filter| remap.expr(filter)).transpose()?,
                ))
            }
            Function::Sliding { aggregate, filter } => {
                Arc::new(SlidingAggregateWindowExpr::new(
                    Arc::new(remap.aggregate(aggregate)?),
                    &partition_by,
                    &order_by,
                    frame,
                    filter.map(|filter| remap.expr(filter)).transpose()?,
                ))
            }
            Function::Udwf(udwf) => Arc::new(StandardWindowExpr::new(
                Arc::new(WindowUDFExpr {
                    args: remap.exprs(udwf.args())?,
                    ..(*udwf).clone()
                }),
                &partition_by,
                &order_by,
                frame,
            )),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::windows::{create_window_expr, window_expr_from_aggregate_expr};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_expr::{WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition};
    use datafusion_functions_aggregate::first_last::first_value_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_functions_window::row_number::row_number_udwf;
    use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};

    /// `p0`, `p1` and `p2` are never read by the windows below.
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("p0", DataType::Utf8, true),
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int64, true),
            Field::new("p1", DataType::Utf8, true),
            Field::new("p2", DataType::Int64, true),
        ]))
    }

    fn rows_frame() -> Arc<WindowFrame> {
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            WindowFrameBound::CurrentRow,
        ))
    }

    fn sort(name: &str, schema: &SchemaRef) -> Result<PhysicalSortExpr> {
        Ok(PhysicalSortExpr::new(
            col(name, schema)?,
            SortOptions::default(),
        ))
    }

    fn sum_c_filtered(schema: &SchemaRef) -> Result<Arc<dyn WindowExpr>> {
        let filter = Arc::new(BinaryExpr::new(col("b", schema)?, Operator::Eq, lit("x")));
        create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            "sum".to_string(),
            &[col("c", schema)?],
            &[col("a", schema)?],
            &[sort("d", schema)?],
            rows_frame(),
            Arc::clone(schema),
            false,
            false,
            Some(filter),
        )
    }

    fn first_c_by_p2(schema: &SchemaRef) -> Result<Arc<dyn WindowExpr>> {
        let aggregate =
            AggregateExprBuilder::new(first_value_udaf(), vec![col("c", schema)?])
                .schema(Arc::clone(schema))
                .alias("first")
                .order_by(vec![sort("p2", schema)?])
                .build()?;
        Ok(window_expr_from_aggregate_expr(
            &[col("a", schema)?],
            &[sort("d", schema)?],
            rows_frame(),
            Arc::new(aggregate),
            None,
        ))
    }

    fn indices(exprs: Vec<Arc<dyn PhysicalExpr>>) -> Vec<usize> {
        let mut used = BTreeSet::new();
        for expr in &exprs {
            assert!(collect_column_indices(expr, &mut used));
        }
        used.into_iter().collect()
    }

    #[test]
    fn keeps_the_columns_every_part_of_the_expressions_reads() -> Result<()> {
        let schema = schema();
        let exprs = [sum_c_filtered(&schema)?, first_c_by_p2(&schema)?];
        let pruned = PrunedInput::new(&exprs, &schema);

        // the filter reads `b`, the aggregate's own ORDER BY reads `p2`
        assert_eq!(pruned.columns.as_deref(), Some(&[1, 2, 3, 4, 6][..]));
        let fields: Vec<_> = pruned
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(fields, ["a", "b", "c", "d", "p2"]);

        let [sum, first] = pruned.window_expr() else {
            panic!("expected two window expressions");
        };
        for (rewritten, original) in pruned.window_expr().iter().zip(&exprs) {
            assert_eq!(rewritten.field()?, original.field()?);
            assert_eq!(rewritten.name(), original.name());
        }
        // `a`, `b`, `c`, `d` and `p2` are now at 0 to 4
        assert_eq!(indices(sum.partition_by().to_vec()), [0]);
        assert_eq!(indices(sum.expressions()), [2]);
        assert_eq!(indices(vec![Arc::clone(&sum.order_by()[0].expr)]), [3]);
        let first_aggregate = first
            .as_any()
            .downcast_ref::<SlidingAggregateWindowExpr>()
            .unwrap()
            .get_aggregate_expr();
        assert_eq!(
            indices(vec![Arc::clone(&first_aggregate.order_bys()[0].expr)]),
            [4]
        );
        let sum_filter = sum
            .as_any()
            .downcast_ref::<SlidingAggregateWindowExpr>()
            .unwrap()
            .get_filter_expr()
            .unwrap();
        assert_eq!(indices(vec![Arc::clone(sum_filter)]), [1]);
        Ok(())
    }

    #[test]
    fn project_keeps_the_columns_and_the_row_count() -> Result<()> {
        let schema = schema();
        let pruned = PrunedInput::new(&[sum_c_filtered(&schema)?], &schema);
        let column = |offset: i64| -> ArrayRef {
            Arc::new(Int64Array::from(vec![offset, offset + 1, offset + 2]))
        };
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            (0..7)
                .map(|i| match schema.field(i).data_type() {
                    DataType::Int64 => column(10 * i as i64),
                    _ => Arc::new(arrow::array::StringArray::from(vec!["x", "y", "x"]))
                        as ArrayRef,
                })
                .collect(),
            &RecordBatchOptions::new(),
        )?;

        let projected = pruned.project(&batch)?;
        assert_eq!(projected.num_rows(), 3);
        assert_eq!(projected.num_columns(), 4);
        assert_eq!(projected.schema(), pruned.schema);
        for (kept, input) in [1, 2, 3, 4].into_iter().enumerate() {
            assert!(Arc::ptr_eq(projected.column(kept), batch.column(input)));
        }
        Ok(())
    }

    #[test]
    fn every_column_read_leaves_the_expressions_as_they_are() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            "sum".to_string(),
            &[col("c", &schema)?],
            &[col("a", &schema)?],
            &[],
            rows_frame(),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;
        let pruned = PrunedInput::new(&[Arc::clone(&expr)], &schema);
        assert!(pruned.columns.is_none());
        assert!(Arc::ptr_eq(&pruned.window_expr()[0], &expr));
        Ok(())
    }

    #[test]
    fn a_function_that_reads_no_column_keeps_none() -> Result<()> {
        let schema = schema();
        let expr = create_window_expr(
            &WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            "row_number".to_string(),
            &[],
            &[],
            &[],
            Arc::new(WindowFrame::new(None)),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;
        let pruned = PrunedInput::new(&[expr], &schema);
        assert_eq!(pruned.columns.as_deref(), Some(&[][..]));

        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            schema
                .fields()
                .iter()
                .map(|f| arrow::array::new_null_array(f.data_type(), 5))
                .collect(),
            &RecordBatchOptions::new(),
        )?;
        let projected = pruned.project(&batch)?;
        assert_eq!(projected.num_columns(), 0);
        assert_eq!(projected.num_rows(), 5);
        Ok(())
    }
}
