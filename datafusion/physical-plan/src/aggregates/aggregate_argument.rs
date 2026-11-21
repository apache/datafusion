// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Evaluation of aggregate function arguments against input batches.

use std::sync::Arc;

use crate::PhysicalExpr;
use arrow::array::{Array, ArrayRef, BooleanArray, NullArray, new_null_array};
use arrow::compute::FilterBuilder;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr_common::utils::scatter;
use itertools::Itertools;

/// Largest literal array, in bytes, that an [`AggregateArgument`] keeps for
/// later batches.
///
/// The kept array lives as long as the aggregate stream and is not tracked by
/// a memory pool, so larger arrays are rebuilt for every batch instead.
const MAX_KEPT_LITERAL_ARRAY_BYTES: usize = 1024 * 1024;

/// One argument of an aggregate function, such as `x` in `SUM(x)`.
///
/// Accumulators take one value per input row, so an argument that evaluates to
/// a single value must be expanded to an array for every batch. A literal
/// argument, such as the `1` in `COUNT(1)` (which is how `COUNT(*)` is planned)
/// or the `','` in `STRING_AGG(x, ',')`, has the same value for every batch of
/// the query. Its array is built on demand and reused for later batches, which
/// receive a zero-copy slice of it.
///
/// Only literals are treated this way. Other expressions are evaluated against
/// every batch: a [`ColumnarValue::Scalar`] result holds one value for the rows
/// of a single batch, and the next batch may produce a different value.
///
/// [`ColumnarValue::Scalar`]: datafusion_expr::ColumnarValue::Scalar
pub(in crate::aggregates) struct AggregateArgument {
    expr: Arc<dyn PhysicalExpr>,
    /// The argument's type in the input schema, also used for rejected rows.
    data_type: DataType,
    /// Set when `expr` is a [`Literal`].
    literal: Option<LiteralArray>,
}

impl AggregateArgument {
    pub(in crate::aggregates) fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Self> {
        let data_type = expr.data_type(input_schema)?;
        let literal = expr
            .downcast_ref::<Literal>()
            .map(|literal| LiteralArray::new(literal.value().clone()));
        Ok(Self {
            expr,
            data_type,
            literal,
        })
    }

    /// Keeps the expression and its type without retaining a literal array.
    pub(in crate::aggregates) fn empty_like(&self) -> Self {
        Self {
            expr: Arc::clone(&self.expr),
            data_type: self.data_type.clone(),
            literal: self
                .literal
                .as_ref()
                .map(|literal| LiteralArray::new(literal.value.clone())),
        }
    }

    pub(in crate::aggregates) fn null_array(&self, num_rows: usize) -> ArrayRef {
        new_null_array(&self.data_type, num_rows)
    }

    pub(in crate::aggregates) fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Evaluates the argument against `batch`, returning one value per row.
    pub(in crate::aggregates) fn evaluate(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<ArrayRef> {
        match &mut self.literal {
            Some(literal) => literal.array_of_size(batch.num_rows()),
            None => self
                .expr
                .evaluate(batch)?
                .into_array_of_size(batch.num_rows()),
        }
    }

    /// Evaluates the argument for the rows of `batch` where `selection` is
    /// true, returning one value per row of `batch`.
    ///
    /// `selected` holds those rows, as returned by [`AggregateFilter::select`].
    /// Like [`PhysicalExpr::evaluate_selection`], the argument is evaluated
    /// only against them, so rejected rows cannot raise errors.
    ///
    /// Rows where `selection` is not true may hold any value, because the
    /// selection is also passed to [`GroupsAccumulator::convert_to_state`],
    /// which ignores them.
    ///
    /// [`GroupsAccumulator::convert_to_state`]: datafusion_expr::GroupsAccumulator::convert_to_state
    pub(in crate::aggregates) fn evaluate_selection(
        &mut self,
        batch: &RecordBatch,
        selection: &BooleanArray,
        selected: &RecordBatch,
    ) -> Result<ArrayRef> {
        let num_rows = batch.num_rows();
        if selected.num_rows() == num_rows {
            return self.evaluate(batch);
        }
        if selected.num_rows() == 0 {
            // A fallible expression may fail even for an empty batch, so it
            // is not evaluated.
            return Ok(self.null_array(num_rows));
        }
        match &mut self.literal {
            Some(literal) => literal.array_of_size(num_rows),
            None => scatter(selection, self.evaluate(selected)?.as_ref()),
        }
    }
}

/// The `FILTER` clause of one aggregate function, such as `x > 0` in
/// `SUM(y) FILTER (WHERE x > 0)`.
///
/// The arguments of the aggregate are evaluated only for the rows the filter
/// selects, so those rows are copied out of each input batch first: evaluating
/// the arguments for every row could raise errors for rejected rows, such as
/// division by zero in `SUM(1 / x) FILTER (WHERE x <> 0)`.
///
/// Only the input columns that the arguments read are copied. A query may have
/// many filtered aggregates over a wide input, and copying every column for
/// every aggregate would dominate the cost of the aggregation.
pub(in crate::aggregates) struct AggregateFilter {
    predicate: Arc<dyn PhysicalExpr>,
    /// Sorted indices of the input columns that the arguments read.
    argument_columns: Vec<usize>,
    /// The input schema last passed to [`Self::select`], and the schema of the
    /// batches it returns for that input.
    selected_schema: Option<(SchemaRef, SchemaRef)>,
}

/// An aggregate filter result, retaining predicates that select or reject a whole batch.
pub(in crate::aggregates) enum AggregateFilterValue {
    Scalar(bool),
    Array(BooleanArray),
}

impl AggregateFilter {
    pub(in crate::aggregates) fn new(
        predicate: Arc<dyn PhysicalExpr>,
        arguments: &[AggregateArgument],
    ) -> Self {
        let argument_columns = arguments
            .iter()
            .flat_map(|argument| collect_columns(argument.expr()))
            .map(|column| column.index())
            .sorted_unstable()
            .dedup()
            .collect();
        Self {
            predicate,
            argument_columns,
            selected_schema: None,
        }
    }

    pub(in crate::aggregates) fn predicate(&self) -> &Arc<dyn PhysicalExpr> {
        &self.predicate
    }

    /// Evaluates the filter predicate for every row of `batch`.
    pub(in crate::aggregates) fn evaluate(
        &self,
        batch: &RecordBatch,
    ) -> Result<AggregateFilterValue> {
        match self.predicate.evaluate(batch)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(value)) => {
                Ok(AggregateFilterValue::Scalar(value.unwrap_or(false)))
            }
            value => {
                let selection = value.into_array(batch.num_rows())?;
                Ok(AggregateFilterValue::Array(
                    as_boolean_array(&selection)?.clone(),
                ))
            }
        }
    }

    /// Returns the rows of `batch` where `selection` is true, for evaluating
    /// the arguments of the aggregate.
    ///
    /// Only the columns that the arguments read are copied. The other columns
    /// are replaced by [`NullArray`]s, which hold no data, so the column
    /// indices in the arguments remain valid. If every row is selected, the
    /// input batch is shared without changing its schema or columns.
    pub(in crate::aggregates) fn select(
        &mut self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<RecordBatch> {
        // Nulls reject their rows, but `has_false` does not count them.
        if selection.null_count() == 0 && !selection.has_false() {
            return Ok(batch.clone());
        }
        let schema = batch.schema_ref();
        // As in `filter_record_batch`, optimizing the filter pays off only
        // when it is applied to several arrays.
        let optimize = match self.argument_columns.as_slice() {
            [] => false,
            [index] => schema.fields().get(*index).is_some_and(|field| {
                FilterBuilder::is_optimize_beneficial(field.data_type())
            }),
            _ => true,
        };
        let mut builder = FilterBuilder::new(selection);
        if optimize {
            builder = builder.optimize();
        }
        let predicate = builder.build();

        let num_rows = predicate.count();
        // Every column the arguments do not read shares one placeholder.
        let placeholder: ArrayRef = Arc::new(NullArray::new(num_rows));
        let mut columns = vec![placeholder; batch.num_columns()];
        for &index in &self.argument_columns {
            let Some(column) = batch.columns().get(index) else {
                return internal_err!(
                    "Aggregate argument references column {index}, but the input has {} columns",
                    batch.num_columns()
                );
            };
            columns[index] = predicate.filter(column.as_ref())?;
        }

        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        Ok(RecordBatch::try_new_with_options(
            self.selected_schema(schema),
            columns,
            &options,
        )?)
    }

    /// Returns `schema` with the fields of columns that the arguments do not
    /// read changed to [`DataType::Null`].
    fn selected_schema(&mut self, schema: &SchemaRef) -> SchemaRef {
        if let Some((input, selected)) = &self.selected_schema
            && input == schema
        {
            return Arc::clone(selected);
        }

        // Schema::project would remove fields and change the column indices
        // used by the existing argument expressions.
        let fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if self.argument_columns.binary_search(&index).is_ok() {
                    Arc::clone(field)
                } else {
                    Arc::new(Field::new(field.name(), DataType::Null, true))
                }
            })
            .collect::<Fields>();
        let selected =
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        self.selected_schema = Some((Arc::clone(schema), Arc::clone(&selected)));
        selected
    }
}

/// A literal value and the largest reusable array retained so far.
struct LiteralArray {
    value: ScalarValue,
    /// `None` until the first array is built, or while every array built has
    /// been larger than [`MAX_KEPT_LITERAL_ARRAY_BYTES`].
    array: Option<ArrayRef>,
}

impl LiteralArray {
    fn new(value: ScalarValue) -> Self {
        Self { value, array: None }
    }

    fn array_of_size(&mut self, num_rows: usize) -> Result<ArrayRef> {
        if let Some(array) = &self.array
            && array.len() >= num_rows
        {
            return Ok(array.slice(0, num_rows));
        }

        let array = self.value.to_array_of_size(num_rows)?;
        if array.get_array_memory_size() <= MAX_KEPT_LITERAL_ARRAY_BYTES {
            self.array = Some(Arc::clone(&array));
        }
        Ok(array)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{AsArray, Int32Array, StringArray};
    use arrow::compute::filter_record_batch;
    use arrow::datatypes::Int32Type;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{binary, col, lit};

    fn batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let values = Int32Array::from_iter_values(0..num_rows as i32);
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    fn values_ptr(array: &ArrayRef) -> *const i32 {
        array.as_primitive::<Int32Type>().values().as_ptr()
    }

    #[test]
    fn literal_array_is_reused_across_batches() -> Result<()> {
        let mut argument = AggregateArgument::try_new(lit(1i32), batch(0).schema_ref())?;

        let first = argument.evaluate(&batch(4))?;
        let smaller = argument.evaluate(&batch(3))?;
        assert_eq!(
            smaller.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1; 3])
        );
        assert_eq!(values_ptr(&smaller), values_ptr(&first));

        // A larger batch builds a longer array, which serves later batches
        let larger = argument.evaluate(&batch(6))?;
        assert_eq!(
            larger.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![1; 6])
        );
        let after_larger = argument.evaluate(&batch(5))?;
        assert_eq!(values_ptr(&after_larger), values_ptr(&larger));

        assert!(argument.evaluate(&batch(0))?.is_empty());
        Ok(())
    }

    #[test]
    fn large_literal_array_is_not_kept() -> Result<()> {
        let mut argument =
            AggregateArgument::try_new(lit("x".repeat(1024)), batch(0).schema_ref())?;

        let first = argument.evaluate(&batch(2048))?;
        let second = argument.evaluate(&batch(2048))?;
        assert_eq!(first.as_ref(), second.as_ref());
        assert_ne!(
            first.as_string::<i32>().values().as_ptr(),
            second.as_string::<i32>().values().as_ptr()
        );
        Ok(())
    }

    #[test]
    fn non_literal_is_evaluated_for_every_batch() -> Result<()> {
        let schema = batch(0).schema();
        let mut argument = AggregateArgument::try_new(col("a", &schema)?, &schema)?;

        let first = argument.evaluate(&batch(3))?;
        assert_eq!(
            first.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![0, 1, 2])
        );
        let second = argument.evaluate(&batch(2))?;
        assert_eq!(
            second.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![0, 1])
        );
        Ok(())
    }

    #[test]
    fn filter_selects_only_argument_columns() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )?;
        let selection =
            BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        let expected = filter_record_batch(&batch, &selection)?;

        // `c + a` reads columns 0 and 2, so column 1 is not copied.
        let expr = binary(
            col("c", &schema)?,
            Operator::Plus,
            col("a", &schema)?,
            &schema,
        )?;
        let mut argument = AggregateArgument::try_new(expr, &schema)?;
        let mut filter = AggregateFilter::new(lit(true), std::slice::from_ref(&argument));
        let selected = filter.select(&batch, &selection)?;
        assert_eq!(selected.num_rows(), 2);
        assert_eq!(selected.column(0), expected.column(0));
        assert_eq!(selected.column(2), expected.column(2));
        assert_eq!(selected.column(1).data_type(), &DataType::Null);
        assert_eq!(selected.column(1).len(), 2);
        assert_eq!(selected.schema().field(1).name(), "b");
        assert_eq!(
            argument.evaluate(&selected)?.as_ref(),
            argument.evaluate(&expected)?.as_ref()
        );

        // The schema of the selected rows is reused for later batches.
        let next =
            filter.select(&batch, &BooleanArray::from(vec![true, true, false, true]))?;
        assert_eq!(next.num_rows(), 3);
        assert!(Arc::ptr_eq(selected.schema_ref(), next.schema_ref()));

        // Selecting every row shares the original arrays and schema.
        let all = filter.select(&batch, &BooleanArray::from(vec![true; 4]))?;
        assert!(Arc::ptr_eq(batch.schema_ref(), all.schema_ref()));
        for (input, selected) in batch.columns().iter().zip(all.columns()) {
            assert!(Arc::ptr_eq(input, selected));
        }

        // `COUNT(*)` reads no columns.
        let mut filter = AggregateFilter::new(
            lit(true),
            &[AggregateArgument::try_new(lit(1), &schema)?],
        );
        let selected = filter.select(&batch, &selection)?;
        assert_eq!(selected.num_rows(), 2);
        assert!(
            selected
                .columns()
                .iter()
                .all(|column| column.data_type() == &DataType::Null)
        );
        Ok(())
    }
}
