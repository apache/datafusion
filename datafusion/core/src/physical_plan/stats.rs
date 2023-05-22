use arrow::array::Array;
use arrow_array::RecordBatch;
use datafusion_common::Statistics;


/// This is a debugging function
///
/// This calculates statistics about a stream of record batches for debugging purposes
#[derive(Debug)]
pub struct StatisticsStream {
    // Map column name --> Statistics
    //cols: BTreeMap<String, ArrayStatistics>
}

impl StatisticsStream {
    pub fn new() -> Self {
        Self {}
    }

    // Record statistics for this batch
    pub fn observe_batch(&self, batch: &RecordBatch) {

        println!("---------------------");
        for (f, array) in batch.schema().fields().iter().zip(batch.columns().iter()) {
            let col_name = f.name();
            let stats = ArrayStatistics::from(array.as_ref());
            println!("Statistics for {col_name}: {}", stats.summary());
        }
    }
}



#[derive(Debug)]
enum ArrayStatistics {
    /// statistics for a dictionaryarray
    Dictionary {
        keys: Statistics,
        values: Statistics,
    },
    Primitive(Statistics),
}


impl ArrayStatistics {
    // TODO make this `impl Display`
    fn summary(&self) -> String {
        match self
        {
            ArrayStatistics::Dictionary { keys, values } => todo!(),
            ArrayStatistics::Primitive(_) => todo!(),
        }
    }
}

impl From<&dyn Array> for ArrayStatistics {
    fn from(value: &dyn Array) -> Self {
        todo!()
    }
}



// TODO make this a sendable record batchs stream so it can be connected to input/output

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::Int32Type;
    use arrow_array::{Int32Array, DictionaryArray};

    use super::ArrayStatistics;


    #[test]
    fn ints() {
        let ints = Int32Array::from(vec![Some(1), None, Some(3)]);
        let stats = ArrayStatistics::from(&ints as &dyn Array);

        assert_eq!("foo", stats.summary())
    }

    #[test]
    fn dictionary() {
        let d1: DictionaryArray<Int32Type> = vec![Some("one"), None, Some("three")].into_iter().collect();

        let stats = ArrayStatistics::from(&d1 as &dyn Array);

        assert_eq!("foo", stats.summary())
    }


}
