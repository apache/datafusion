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

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Schema},
};
use datafusion_common::Column;
use datafusion_common::ScalarValue;
use log::debug;

use parquet::file::{
    metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics,
};

use crate::datasource::physical_plan::parquet::{
    from_bytes_to_i128, parquet_to_arrow_decimal_type,
};
use crate::{
    datasource::listing::FileRange,
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
};

use super::ParquetFileMetrics;

/// Returns a vector of indexes into `groups` which should be scanned.
///
/// If an index is NOT present in the returned Vec it means the
/// predicate filtered all the row group.
///
/// If an index IS present in the returned Vec it means the predicate
/// did not filter out that row group.
pub(crate) fn prune_row_groups(
    groups: &[RowGroupMetaData],
    range: Option<FileRange>,
    predicate: Option<&PruningPredicate>,
    metrics: &ParquetFileMetrics,
) -> Vec<usize> {
    let mut filtered = Vec::with_capacity(groups.len());
    for (idx, metadata) in groups.iter().enumerate() {
        if let Some(range) = &range {
            // figure out where the first dictionary page (or first data page are)
            // note don't use the location of metadata
            // <https://github.com/apache/arrow-datafusion/issues/5995>
            let col = metadata.column(0);
            let offset = col
                .dictionary_page_offset()
                .unwrap_or_else(|| col.data_page_offset());
            if offset < range.start || offset >= range.end {
                continue;
            }
        }

        if let Some(predicate) = predicate {
            let pruning_stats = RowGroupPruningStatistics {
                row_group_metadata: metadata,
                parquet_schema: predicate.schema().as_ref(),
            };
            match predicate.prune(&pruning_stats) {
                Ok(values) => {
                    // NB: false means don't scan row group
                    if !values[0] {
                        metrics.row_groups_pruned.add(1);
                        continue;
                    }
                }
                // stats filter array could not be built
                // return a closure which will not filter out any row groups
                Err(e) => {
                    debug!("Error evaluating row group predicate values {e}");
                    metrics.predicate_evaluation_errors.add(1);
                }
            }
        }

        filtered.push(idx)
    }
    filtered
}

/// Wraps parquet statistics in a way
/// that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    row_group_metadata: &'a RowGroupMetaData,
    parquet_schema: &'a Schema,
}

/// Extract the min/max statistics from a `ParquetStatistics` object
macro_rules! get_statistic {
    ($column_statistics:expr, $func:ident, $bytes_func:ident, $target_arrow_type:expr) => {{
        if !$column_statistics.has_min_max_set() {
            return None;
        }
        match $column_statistics {
            ParquetStatistics::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.$func()))),
            ParquetStatistics::Int32(s) => {
                match $target_arrow_type {
                    // int32 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(*s.$func() as i128),
                            precision,
                            scale,
                        ))
                    }
                    _ => Some(ScalarValue::Int32(Some(*s.$func()))),
                }
            }
            ParquetStatistics::Int64(s) => {
                match $target_arrow_type {
                    // int64 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(*s.$func() as i128),
                            precision,
                            scale,
                        ))
                    }
                    _ => Some(ScalarValue::Int64(Some(*s.$func()))),
                }
            }
            // 96 bit ints not supported
            ParquetStatistics::Int96(_) => None,
            ParquetStatistics::Float(s) => Some(ScalarValue::Float32(Some(*s.$func()))),
            ParquetStatistics::Double(s) => Some(ScalarValue::Float64(Some(*s.$func()))),
            ParquetStatistics::ByteArray(s) => {
                match $target_arrow_type {
                    // decimal data type
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(from_bytes_to_i128(s.$bytes_func())),
                            precision,
                            scale,
                        ))
                    }
                    _ => {
                        let s = std::str::from_utf8(s.$bytes_func())
                            .map(|s| s.to_string())
                            .ok();
                        Some(ScalarValue::Utf8(s))
                    }
                }
            }
            // type not supported yet
            ParquetStatistics::FixedLenByteArray(s) => {
                match $target_arrow_type {
                    // just support the decimal data type
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(from_bytes_to_i128(s.$bytes_func())),
                            precision,
                            scale,
                        ))
                    }
                    _ => None,
                }
            }
        }
    }};
}

// Extract the min or max value calling `func` or `bytes_func` on the ParquetStatistics as appropriate
macro_rules! get_min_max_values {
    ($self:expr, $column:expr, $func:ident, $bytes_func:ident) => {{
        let (_column_index, field) =
            if let Some((v, f)) = $self.parquet_schema.column_with_name(&$column.unqualified_name()) {
                (v, f)
            } else {
                // Named column was not present
                return None;
            };

        let data_type = field.data_type();
        // The result may be None, because DataFusion doesn't have support for ScalarValues of the column type
        let null_scalar: ScalarValue = data_type.try_into().ok()?;

        $self.row_group_metadata
            .columns()
            .iter()
            .find(|c| c.column_descr().name() == &$column.unqualified_name())
            .and_then(|c| if c.statistics().is_some() {Some((c.statistics().unwrap(), c.column_descr()))} else {None})
            .map(|(stats, column_descr)|
                {
                    let target_data_type = parquet_to_arrow_decimal_type(column_descr);
                    get_statistic!(stats, $func, $bytes_func, target_data_type)
                })
            .flatten()
            // column either didn't have statistics at all or didn't have min/max values
            .or_else(|| Some(null_scalar.clone()))
            .map(|s| s.to_array())
    }}
}

// Extract the null count value on the ParquetStatistics
macro_rules! get_null_count_values {
    ($self:expr, $column:expr) => {{
        let value = ScalarValue::UInt64(
            if let Some(col) = $self
                .row_group_metadata
                .columns()
                .iter()
                .find(|c| c.column_descr().name() == &$column.unqualified_name())
            {
                col.statistics().map(|s| s.null_count())
            } else {
                Some($self.row_group_metadata.num_rows() as u64)
            },
        );

        Some(value.to_array())
    }};
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, min, min_bytes)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, max, max_bytes)
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        get_null_count_values!(self, column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::metrics::ExecutionPlanMetricsSet;
    use arrow::datatypes::DataType::Decimal128;
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::{cast, col, lit, Expr};
    use datafusion_physical_expr::execution_props::ExecutionProps;
    use datafusion_physical_expr::{create_physical_expr, PhysicalExpr};
    use parquet::basic::LogicalType;
    use parquet::data_type::{ByteArray, FixedLenByteArray};
    use parquet::{
        basic::Type as PhysicalType,
        file::{metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics},
        schema::types::SchemaDescPtr,
    };
    use std::ops::Rem;
    use std::sync::Arc;

    struct PrimitiveTypeField {
        name: &'static str,
        physical_ty: PhysicalType,
        logical_ty: Option<LogicalType>,
        precision: Option<i32>,
        scale: Option<i32>,
        byte_len: Option<i32>,
    }

    impl PrimitiveTypeField {
        fn new(name: &'static str, physical_ty: PhysicalType) -> Self {
            Self {
                name,
                physical_ty,
                logical_ty: None,
                precision: None,
                scale: None,
                byte_len: None,
            }
        }

        fn with_logical_type(mut self, logical_type: LogicalType) -> Self {
            self.logical_ty = Some(logical_type);
            self
        }

        fn with_precision(mut self, precision: i32) -> Self {
            self.precision = Some(precision);
            self
        }

        fn with_scale(mut self, scale: i32) -> Self {
            self.scale = Some(scale);
            self
        }

        fn with_byte_len(mut self, byte_len: i32) -> Self {
            self.byte_len = Some(byte_len);
            self
        }
    }

    #[test]
    fn row_group_pruning_predicate_simple_expr() {
        use datafusion_expr::{col, lit};
        // int > 1 => c1_max > 1
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expr = col("c1").gt(lit(15));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();

        let field = PrimitiveTypeField::new("c1", PhysicalType::INT32);
        let schema_descr = get_test_schema_descr(vec![field]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(1), Some(10), None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );

        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(&[rgm1, rgm2], None, Some(&pruning_predicate), &metrics),
            vec![1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_missing_stats() {
        use datafusion_expr::{col, lit};
        // int > 1 => c1_max > 1
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let expr = col("c1").gt(lit(15));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();

        let field = PrimitiveTypeField::new("c1", PhysicalType::INT32);
        let schema_descr = get_test_schema_descr(vec![field]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(None, None, None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        // missing statistics for first row group mean that the result from the predicate expression
        // is null / undefined so the first row group can't be filtered out
        assert_eq!(
            prune_row_groups(&[rgm1, rgm2], None, Some(&pruning_predicate), &metrics),
            vec![0, 1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_partial_expr() {
        use datafusion_expr::{col, lit};
        // test row group predicate with partially supported expression
        // (int > 1) and ((int % 2) = 0) => c1_max > 1 and true
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]));
        let expr = col("c1").gt(lit(15)).and(col("c2").rem(lit(2)).eq(lit(0)));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate = PruningPredicate::try_new(expr, schema.clone()).unwrap();

        let schema_descr = get_test_schema_descr(vec![
            PrimitiveTypeField::new("c1", PhysicalType::INT32),
            PrimitiveTypeField::new("c2", PhysicalType::INT32),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
            ],
        );

        let metrics = parquet_file_metrics();
        let groups = &[rgm1, rgm2];
        // the first row group is still filtered out because the predicate expression can be partially evaluated
        // when conditions are joined using AND
        assert_eq!(
            prune_row_groups(groups, None, Some(&pruning_predicate), &metrics),
            vec![1]
        );

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        let expr = col("c1").gt(lit(15)).or(col("c2").rem(lit(2)).eq(lit(0)));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate = PruningPredicate::try_new(expr, schema).unwrap();

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        assert_eq!(
            prune_row_groups(groups, None, Some(&pruning_predicate), &metrics),
            vec![0, 1]
        );
    }

    fn gen_row_group_meta_data_for_pruning_predicate() -> Vec<RowGroupMetaData> {
        let schema_descr = get_test_schema_descr(vec![
            PrimitiveTypeField::new("c1", PhysicalType::INT32),
            PrimitiveTypeField::new("c2", PhysicalType::BOOLEAN),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 1, false),
            ],
        );
        vec![rgm1, rgm2]
    }

    #[test]
    fn row_group_pruning_predicate_null_expr() {
        use datafusion_expr::{col, lit};
        // int > 1 and IsNull(bool) => c1_max > 1 and bool_null_count > 0
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]));
        let expr = col("c1").gt(lit(15)).and(col("c2").is_null());
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate = PruningPredicate::try_new(expr, schema).unwrap();
        let groups = gen_row_group_meta_data_for_pruning_predicate();

        let metrics = parquet_file_metrics();
        // First row group was filtered out because it contains no null value on "c2".
        assert_eq!(
            prune_row_groups(&groups, None, Some(&pruning_predicate), &metrics),
            vec![1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_eq_null_expr() {
        use datafusion_expr::{col, lit};
        // test row group predicate with an unknown (Null) expr
        //
        // int > 1 and bool = NULL => c1_max > 1 and null
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]));
        let expr = col("c1")
            .gt(lit(15))
            .and(col("c2").eq(lit(ScalarValue::Boolean(None))));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate = PruningPredicate::try_new(expr, schema).unwrap();
        let groups = gen_row_group_meta_data_for_pruning_predicate();

        let metrics = parquet_file_metrics();
        // bool = NULL always evaluates to NULL (and thus will not
        // pass predicates. Ideally these should both be false
        assert_eq!(
            prune_row_groups(&groups, None, Some(&pruning_predicate), &metrics),
            vec![1]
        );
    }

    #[test]
    fn row_group_pruning_predicate_decimal_type() {
        // For the decimal data type, parquet can use `INT32`, `INT64`, `BYTE_ARRAY`, `FIXED_LENGTH_BYTE_ARRAY` to
        // store the data.
        // In this case, construct four types of statistics to filtered with the decimal predication.

        // INT32: c1 > 5, the c1 is decimal(9,2)
        // The type of scalar value if decimal(9,2), don't need to do cast
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(9, 2), false)]);
        let field = PrimitiveTypeField::new("c1", PhysicalType::INT32)
            .with_logical_type(LogicalType::Decimal {
                scale: 2,
                precision: 9,
            })
            .with_scale(2)
            .with_precision(9);
        let schema_descr = get_test_schema_descr(vec![field]);
        let expr = col("c1").gt(lit(ScalarValue::Decimal128(Some(500), 9, 2)));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            // [1.00, 6.00]
            // c1 > 5, this row group will be included in the results.
            vec![ParquetStatistics::int32(
                Some(100),
                Some(600),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            // [0.1, 0.2]
            // c1 > 5, this row group will not be included in the results.
            vec![ParquetStatistics::int32(Some(10), Some(20), None, 0, false)],
        );
        let rgm3 = get_row_group_meta_data(
            &schema_descr,
            // [1, None]
            // c1 > 5, this row group can not be filtered out, so will be included in the results.
            vec![ParquetStatistics::int32(Some(100), None, None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(
                &[rgm1, rgm2, rgm3],
                None,
                Some(&pruning_predicate),
                &metrics
            ),
            vec![0, 2]
        );

        // INT32: c1 > 5, but parquet decimal type has different precision or scale to arrow decimal
        // The c1 type is decimal(9,0) in the parquet file, and the type of scalar is decimal(5,2).
        // We should convert all type to the coercion type, which is decimal(11,2)
        // The decimal of arrow is decimal(5,2), the decimal of parquet is decimal(9,0)
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(9, 0), false)]);

        let field = PrimitiveTypeField::new("c1", PhysicalType::INT32)
            .with_logical_type(LogicalType::Decimal {
                scale: 0,
                precision: 9,
            })
            .with_scale(0)
            .with_precision(9);
        let schema_descr = get_test_schema_descr(vec![field]);
        let expr = cast(col("c1"), DataType::Decimal128(11, 2)).gt(cast(
            lit(ScalarValue::Decimal128(Some(500), 5, 2)),
            Decimal128(11, 2),
        ));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            // [100, 600]
            // c1 > 5, this row group will be included in the results.
            vec![ParquetStatistics::int32(
                Some(100),
                Some(600),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            // [10, 20]
            // c1 > 5, this row group will be included in the results.
            vec![ParquetStatistics::int32(Some(10), Some(20), None, 0, false)],
        );
        let rgm3 = get_row_group_meta_data(
            &schema_descr,
            // [0, 2]
            // c1 > 5, this row group will not be included in the results.
            vec![ParquetStatistics::int32(Some(0), Some(2), None, 0, false)],
        );
        let rgm4 = get_row_group_meta_data(
            &schema_descr,
            // [None, 2]
            // c1 > 5, this row group can not be filtered out, so will be included in the results.
            vec![ParquetStatistics::int32(None, Some(2), None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(
                &[rgm1, rgm2, rgm3, rgm4],
                None,
                Some(&pruning_predicate),
                &metrics
            ),
            vec![0, 1, 3]
        );

        // INT64: c1 < 5, the c1 is decimal(18,2)
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(18, 2), false)]);
        let field = PrimitiveTypeField::new("c1", PhysicalType::INT64)
            .with_logical_type(LogicalType::Decimal {
                scale: 2,
                precision: 18,
            })
            .with_scale(2)
            .with_precision(18);
        let schema_descr = get_test_schema_descr(vec![field]);
        let expr = col("c1").lt(lit(ScalarValue::Decimal128(Some(500), 18, 2)));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            // [6.00, 8.00]
            vec![ParquetStatistics::int32(
                Some(600),
                Some(800),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            // [0.1, 0.2]
            vec![ParquetStatistics::int64(Some(10), Some(20), None, 0, false)],
        );
        let rgm3 = get_row_group_meta_data(
            &schema_descr,
            // [0.1, 0.2]
            vec![ParquetStatistics::int64(None, None, None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(
                &[rgm1, rgm2, rgm3],
                None,
                Some(&pruning_predicate),
                &metrics
            ),
            vec![1, 2]
        );

        // FIXED_LENGTH_BYTE_ARRAY: c1 = decimal128(100000, 28, 3), the c1 is decimal(18,2)
        // the type of parquet is decimal(18,2)
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(18, 2), false)]);
        let field = PrimitiveTypeField::new("c1", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_logical_type(LogicalType::Decimal {
                scale: 2,
                precision: 18,
            })
            .with_scale(2)
            .with_precision(18)
            .with_byte_len(16);
        let schema_descr = get_test_schema_descr(vec![field]);
        // cast the type of c1 to decimal(28,3)
        let left = cast(col("c1"), DataType::Decimal128(28, 3));
        let expr = left.eq(lit(ScalarValue::Decimal128(Some(100000), 28, 3)));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        // we must use the big-endian when encode the i128 to bytes or vec[u8].
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::fixed_len_byte_array(
                // 5.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    500i128.to_be_bytes().to_vec(),
                ))),
                // 80.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    8000i128.to_be_bytes().to_vec(),
                ))),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::fixed_len_byte_array(
                // 5.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    500i128.to_be_bytes().to_vec(),
                ))),
                // 200.00
                Some(FixedLenByteArray::from(ByteArray::from(
                    20000i128.to_be_bytes().to_vec(),
                ))),
                None,
                0,
                false,
            )],
        );

        let rgm3 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::fixed_len_byte_array(
                None, None, None, 0, false,
            )],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(
                &[rgm1, rgm2, rgm3],
                None,
                Some(&pruning_predicate),
                &metrics
            ),
            vec![1, 2]
        );

        // BYTE_ARRAY: c1 = decimal128(100000, 28, 3), the c1 is decimal(18,2)
        // the type of parquet is decimal(18,2)
        let schema =
            Schema::new(vec![Field::new("c1", DataType::Decimal128(18, 2), false)]);
        let field = PrimitiveTypeField::new("c1", PhysicalType::BYTE_ARRAY)
            .with_logical_type(LogicalType::Decimal {
                scale: 2,
                precision: 18,
            })
            .with_scale(2)
            .with_precision(18)
            .with_byte_len(16);
        let schema_descr = get_test_schema_descr(vec![field]);
        // cast the type of c1 to decimal(28,3)
        let left = cast(col("c1"), DataType::Decimal128(28, 3));
        let expr = left.eq(lit(ScalarValue::Decimal128(Some(100000), 28, 3)));
        let expr = logical2physical(&expr, &schema);
        let pruning_predicate =
            PruningPredicate::try_new(expr, Arc::new(schema)).unwrap();
        // we must use the big-endian when encode the i128 to bytes or vec[u8].
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::byte_array(
                // 5.00
                Some(ByteArray::from(500i128.to_be_bytes().to_vec())),
                // 80.00
                Some(ByteArray::from(8000i128.to_be_bytes().to_vec())),
                None,
                0,
                false,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::byte_array(
                // 5.00
                Some(ByteArray::from(500i128.to_be_bytes().to_vec())),
                // 200.00
                Some(ByteArray::from(20000i128.to_be_bytes().to_vec())),
                None,
                0,
                false,
            )],
        );
        let rgm3 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::byte_array(None, None, None, 0, false)],
        );
        let metrics = parquet_file_metrics();
        assert_eq!(
            prune_row_groups(
                &[rgm1, rgm2, rgm3],
                None,
                Some(&pruning_predicate),
                &metrics
            ),
            vec![1, 2]
        );
    }

    fn get_row_group_meta_data(
        schema_descr: &SchemaDescPtr,
        column_statistics: Vec<ParquetStatistics>,
    ) -> RowGroupMetaData {
        use parquet::file::metadata::ColumnChunkMetaData;
        let mut columns = vec![];
        for (i, s) in column_statistics.iter().enumerate() {
            let column = ColumnChunkMetaData::builder(schema_descr.column(i))
                .set_statistics(s.clone())
                .build()
                .unwrap();
            columns.push(column);
        }
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr(fields: Vec<PrimitiveTypeField>) -> SchemaDescPtr {
        use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};
        let schema_fields = fields
            .iter()
            .map(|field| {
                let mut builder =
                    SchemaType::primitive_type_builder(field.name, field.physical_ty);
                // add logical type for the parquet field
                if let Some(logical_type) = &field.logical_ty {
                    builder = builder.with_logical_type(Some(logical_type.clone()));
                }
                if let Some(precision) = field.precision {
                    builder = builder.with_precision(precision);
                }
                if let Some(scale) = field.scale {
                    builder = builder.with_scale(scale);
                }
                if let Some(byte_len) = field.byte_len {
                    builder = builder.with_length(byte_len);
                }
                Arc::new(builder.build().unwrap())
            })
            .collect::<Vec<_>>();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(schema_fields)
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    fn parquet_file_metrics() -> ParquetFileMetrics {
        let metrics = Arc::new(ExecutionPlanMetricsSet::new());
        ParquetFileMetrics::new(0, "file.parquet", &metrics)
    }

    fn logical2physical(expr: &Expr, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let df_schema = schema.clone().to_dfschema().unwrap();
        let execution_props = ExecutionProps::new();
        create_physical_expr(expr, &df_schema, schema, &execution_props).unwrap()
    }
}
