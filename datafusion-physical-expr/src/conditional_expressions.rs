use std::sync::Arc;

use arrow::array::{Array, StringArray};

use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

pub fn coalesce(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(format!(
            "coalesce was called with {} arguments. It requires at least 1.",
            args.len()
        )));
    }

    let mut res = vec![];
    let size = match args[0] {
        ColumnarValue::Array(ref a) => a.len(),
        ColumnarValue::Scalar(ref _s) => 1,
    };

    for i in 0..size {
        let mut value = None;
        for column_value in args {
            match column_value {
                ColumnarValue::Array(array_ref) => {
                    if array_ref.is_valid(i) {
                        let array =
                            array_ref.as_any().downcast_ref::<StringArray>().unwrap();
                        value = Some(array.value(i));
                        break;
                    }
                }
                ColumnarValue::Scalar(scalar) => match scalar {
                    ScalarValue::Utf8(data) => {
                        value = match data {
                            None => None,
                            Some(str) => Some(str.as_ref()),
                        };
                        // break only if there's a non null value
                        if value.is_some() {
                            break;
                        }
                    }
                    _ => {
                        continue;
                    }
                },
            }
        }
        res.push(value);
    }

    Ok(ColumnarValue::Array(Arc::new(StringArray::from(res))))
}
