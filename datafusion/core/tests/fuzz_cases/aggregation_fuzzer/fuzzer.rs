use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::prelude::SessionContext;
use tokio::task::JoinSet;

use crate::fuzz_cases::aggregation_fuzzer::{
    context_generator::SessionContextGenerator, data_generator::DatasetGenerator,
};

struct AggregationFuzzer {
    /// Rounds to call `generate` of `DataSetsGenerator`
    /// `len(sort_keys_set) + 1` datasets will generated.
    data_gen_rounds: usize,

    /// Rounds to call `generate` of `SessionContextGenerator`
    /// `ctx_gen_rounds` datasets will generated.
    ctx_gen_rounds: usize,

    query: Arc<String>,

    dataset_generator: DatasetGenerator,

    ctx_generator: SessionContextGenerator,
}

impl AggregationFuzzer {
    pub async fn run(&self) {
        


        let mut join_set = JoinSet::new();
    }
}

struct AggregationFuzzTestCase {
    /// Expected result in current test case
    /// It is generate from `query` + `baseline session context`
    expected_result: Arc<Vec<RecordBatch>>,

    /// The test query
    /// Use sql to represent it currently.
    query: Arc<String>,

    /// Generated session context in current test case
    ctx: SessionContext,
}
