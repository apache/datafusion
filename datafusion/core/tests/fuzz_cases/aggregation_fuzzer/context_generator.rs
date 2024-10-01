use datafusion::prelude::SessionContext;
use rand::thread_rng;

#[derive(Debug, Clone)]
pub struct SessionContextGeneratorBuilder {
    total_rows_num: usize,
    sort_keys: Vec<Vec<String>>,
}

pub struct SessionContextGenerator {
    /// Used in generate the random `batch_size`
    ///
    /// The generated `batch_size` is between (0, total_rows_num]
    total_rows_num: usize,

    sort_keys: Vec<Vec<String>>,


}

struct SkipPartialParams {
    
}

impl SessionContextGenerator {
    pub fn generate(&self) -> SessionContext {
        let mut rng = thread_rng();
    }
}


