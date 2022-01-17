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

//! Fork of arrow::io::print to implement custom Binary Array formatting logic.

// adapted from https://github.com/jorgecarleitao/arrow2/blob/ef7937dfe56033c2cc491482c67587b52cd91554/src/array/display.rs
// see: https://github.com/jorgecarleitao/arrow2/issues/771

use arrow::{array::*, record_batch::RecordBatch};

use comfy_table::{Cell, Table};

macro_rules! dyn_display {
    ($array:expr, $ty:ty, $expr:expr) => {{
        let a = $array.as_any().downcast_ref::<$ty>().unwrap();
        Box::new(move |row: usize| format!("{}", $expr(a.value(row))))
    }};
}

fn df_get_array_value_display<'a>(
    array: &'a dyn Array,
) -> Box<dyn Fn(usize) -> String + 'a> {
    use arrow::datatypes::DataType::*;
    match array.data_type() {
        Binary => dyn_display!(array, BinaryArray<i32>, |x: &[u8]| {
            x.iter().fold("".to_string(), |mut acc, x| {
                acc.push_str(&format!("{:02x}", x));
                acc
            })
        }),
        LargeBinary => dyn_display!(array, BinaryArray<i64>, |x: &[u8]| {
            x.iter().fold("".to_string(), |mut acc, x| {
                acc.push_str(&format!("{:02x}", x));
                acc
            })
        }),
        List(_) => {
            let f = |x: Box<dyn Array>| {
                let display = df_get_array_value_display(x.as_ref());
                let string_values = (0..x.len()).map(display).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, ListArray<i32>, f)
        }
        FixedSizeList(_, _) => {
            let f = |x: Box<dyn Array>| {
                let display = df_get_array_value_display(x.as_ref());
                let string_values = (0..x.len()).map(display).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, FixedSizeListArray, f)
        }
        LargeList(_) => {
            let f = |x: Box<dyn Array>| {
                let display = df_get_array_value_display(x.as_ref());
                let string_values = (0..x.len()).map(display).collect::<Vec<String>>();
                format!("[{}]", string_values.join(", "))
            };
            dyn_display!(array, ListArray<i64>, f)
        }
        Struct(_) => {
            let a = array.as_any().downcast_ref::<StructArray>().unwrap();
            let displays = a
                .values()
                .iter()
                .map(|x| df_get_array_value_display(x.as_ref()))
                .collect::<Vec<_>>();
            Box::new(move |row: usize| {
                let mut string = displays
                    .iter()
                    .zip(a.fields().iter().map(|f| f.name()))
                    .map(|(f, name)| (f(row), name))
                    .fold("{".to_string(), |mut acc, (v, name)| {
                        acc.push_str(&format!("{}: {}, ", name, v));
                        acc
                    });
                if string.len() > 1 {
                    // remove last ", "
                    string.pop();
                    string.pop();
                }
                string.push('}');
                string
            })
        }
        _ => get_display(array),
    }
}

/// Returns a function of index returning the string representation of the item of `array`.
/// This outputs an empty string on nulls.
pub fn df_get_display<'a>(array: &'a dyn Array) -> Box<dyn Fn(usize) -> String + 'a> {
    let value_display = df_get_array_value_display(array);
    Box::new(move |row| {
        if array.is_null(row) {
            "".to_string()
        } else {
            value_display(row)
        }
    })
}

/// Convert a series of record batches into a String
pub fn write(results: &[RecordBatch]) -> String {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return table.to_string();
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    for batch in results {
        let displayes = batch
            .columns()
            .iter()
            .map(|array| df_get_display(array.as_ref()))
            .collect::<Vec<_>>();

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            (0..batch.num_columns()).for_each(|col| {
                let string = displayes[col](row);
                cells.push(Cell::new(&string));
            });
            table.add_row(cells);
        }
    }
    table.to_string()
}
