use datafusion::arrow::{
    array::{Array, Decimal128Builder},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;

// TODO: move this to datafusion::test_utils?
pub fn make_decimal() -> RecordBatch {
    let mut decimal_builder = Decimal128Builder::with_capacity(20);
    for i in 110000..110010 {
        decimal_builder.append_value(i as i128);
    }
    for i in 100000..100010 {
        decimal_builder.append_value(-i as i128);
    }
    let array = decimal_builder
        .finish()
        .with_precision_and_scale(10, 3)
        .unwrap();
    let schema = Schema::new(vec![Field::new("c1", array.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}
