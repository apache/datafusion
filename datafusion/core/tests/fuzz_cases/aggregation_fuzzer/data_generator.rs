use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion_physical_plan::sorts::sort::sort_batch;
use rand::{
    rngs::{StdRng, ThreadRng},
    thread_rng, Rng, SeedableRng,
};
use rand_distr::Alphanumeric;
use test_utils::{
    array_gen::{PrimitiveArrayGenerator, StringArrayGenerator},
    StringBatchGenerator,
};

// Data set generator
// It will generate data set according to:
//   - Seed
//   - DataSetGeneratorConfig
struct DataSetsGenerator {
    config: DataSetGeneratorConfig,
    batch_generator: RecordBatchGenerator,
}

impl DataSetsGenerator {
    fn new(config: DataSetGeneratorConfig) -> Self {
        let batch_generator = RecordBatchGenerator::new(
            config.min_rows_num,
            config.max_rows_num,
            config.columns.clone(),
        );

        Self {
            config,
            batch_generator,
        }
    }

    fn generate(&self) -> Vec<DataSet> {
        let batch = self.batch_generator.generate();
        vec![DataSet {
            batches: vec![batch],
            sorted_key: None,
        }]
    }
}

/// Single test data set
#[derive(Debug)]
struct DataSet {
    batches: Vec<RecordBatch>,
    sorted_key: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct DataSetGeneratorConfig {
    // Each generated column in data set
    columns: Vec<ColumnDescr>,

    // Min rows num
    min_rows_num: usize,

    // Max rows num
    max_rows_num: usize,

    // Sort keys used to generate the sorted data set
    sort_keys: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
struct ColumnDescr {
    // Column name
    name: String,

    // Data type of this column
    column_type: DataType,
}

/// Record batch generator
struct RecordBatchGenerator {
    min_rows_nun: usize,

    max_rows_num: usize,

    columns: Vec<ColumnDescr>,

    candidate_null_pcts: Vec<f64>,
}

macro_rules! generate_string_array {
    ($SELF:ident, $NUM_ROWS:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $OFFSET_TYPE:ty) => {{
        let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
        let null_pct = $SELF.candidate_null_pcts[null_pct_idx];
        let max_len = $BATCH_GEN_RNG.gen_range(1..50);
        let num_distinct_strings = if $NUM_ROWS > 1 {
            $BATCH_GEN_RNG.gen_range(1..$NUM_ROWS)
        } else {
            $NUM_ROWS
        };

        let mut generator = StringArrayGenerator {
            max_len,
            num_strings: $NUM_ROWS,
            num_distinct_strings,
            null_pct,
            rng: $ARRAY_GEN_RNG,
        };

        generator.gen_data::<$OFFSET_TYPE>()
    }};
}

macro_rules! generate_primitive_array {
    ($SELF:ident, $NUM_ROWS:ident, $BATCH_GEN_RNG:ident, $ARRAY_GEN_RNG:ident, $DATA_TYPE:ident) => {
        paste::paste! {{
            let null_pct_idx = $BATCH_GEN_RNG.gen_range(0..$SELF.candidate_null_pcts.len());
            let null_pct = $SELF.candidate_null_pcts[null_pct_idx];
            let num_distinct_primitives = if $NUM_ROWS > 1 {
                $BATCH_GEN_RNG.gen_range(1..$NUM_ROWS)
            } else {
                $NUM_ROWS
            };

            let mut generator = PrimitiveArrayGenerator {
                num_primitives: $NUM_ROWS,
                num_distinct_primitives,
                null_pct,
                rng: $ARRAY_GEN_RNG,
            };

            generator.[< gen_data_ $DATA_TYPE >]()
    }}}
}

impl RecordBatchGenerator {
    fn new(min_rows_nun: usize, max_rows_num: usize, columns: Vec<ColumnDescr>) -> Self {
        let candidate_null_pcts = vec![0.0, 0.01, 0.1, 0.5];

        Self {
            min_rows_nun,
            max_rows_num,
            columns,
            candidate_null_pcts,
        }
    }

    fn generate(&self) -> RecordBatch {
        let mut rng = thread_rng();
        let num_rows = rng.gen_range(self.min_rows_nun..=self.max_rows_num);
        let array_gen_rng = StdRng::from_seed(rng.gen());

        // Build arrays
        let mut arrays = Vec::with_capacity(self.columns.len());
        for col in self.columns.iter() {
            let array = self.generate_array_of_type(
                col.column_type.clone(),
                num_rows,
                &mut rng,
                array_gen_rng.clone(),
            );
            arrays.push(array);
        }

        // Build schema
        let fields = self
            .columns
            .iter()
            .map(|col| Field::new(col.name.clone(), col.column_type.clone(), true))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));

        RecordBatch::try_new(schema, arrays).unwrap()
    }

    fn generate_array_of_type(
        &self,
        data_type: DataType,
        num_rows: usize,
        batch_gen_rng: &mut ThreadRng,
        array_gen_rng: StdRng,
    ) -> ArrayRef {
        match data_type {
            DataType::Int8 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    i8
                )
            }
            DataType::Int16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    i16
                )
            }
            DataType::Int32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    i32
                )
            }
            DataType::Int64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    i64
                )
            }
            DataType::UInt8 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u8
                )
            }
            DataType::UInt16 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u16
                )
            }
            DataType::UInt32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u32
                )
            }
            DataType::UInt64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    u64
                )
            }
            DataType::Float32 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    f32
                )
            }
            DataType::Float64 => {
                generate_primitive_array!(
                    self,
                    num_rows,
                    batch_gen_rng,
                    array_gen_rng,
                    f64
                )
            }
            DataType::Utf8 => {
                generate_string_array!(self, num_rows, batch_gen_rng, array_gen_rng, i32)
            }
            DataType::LargeUtf8 => {
                generate_string_array!(self, num_rows, batch_gen_rng, array_gen_rng, i64)
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use arrow::util::pretty::pretty_format_batches;

    use super::*;

    #[test]
    fn simple_test() {
        let config = DataSetGeneratorConfig {
            columns: vec![
                ColumnDescr {
                    name: "a".to_string(),
                    column_type: DataType::Utf8,
                },
                ColumnDescr {
                    name: "b".to_string(),
                    column_type: DataType::UInt32,
                },
            ],
            min_rows_num: 16,
            max_rows_num: 32,
            sort_keys: Vec::new(),
        };

        let gen = DataSetsGenerator::new(config);
        let data_sets = gen.generate();
        println!("{}", pretty_format_batches(&data_sets[0].batches).unwrap());
    }
}
