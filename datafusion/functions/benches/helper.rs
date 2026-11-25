// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{StringArray, StringViewArray};
use datafusion_expr::ColumnarValue;
use rand::distr::Alphanumeric;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::sync::Arc;

/// gen_arr(4096, 128, 0.1, 0.1, true) will generate a StringViewArray with
/// 4096 rows, each row containing a string with 128 random characters.
/// around 10% of the rows are null, around 10% of the rows are non-ASCII.
pub fn gen_string_array(
    n_rows: usize,
    str_len_chars: usize,
    null_density: f32,
    utf8_density: f32,
    is_string_view: bool, // false -> StringArray, true -> StringViewArray
) -> Vec<ColumnarValue> {
    let mut rng = StdRng::seed_from_u64(42);
    let rng_ref = &mut rng;

    let corpus = "DataFusionДатаФусион数据融合📊🔥"; // includes utf8 encoding with 1~4 bytes
    let corpus = corpus.chars().collect::<Vec<_>>();

    let mut output_string_vec: Vec<Option<String>> = Vec::with_capacity(n_rows);
    for _ in 0..n_rows {
        let rand_num = rng_ref.random::<f32>(); // [0.0, 1.0)
        if rand_num < null_density {
            output_string_vec.push(None);
        } else if rand_num < null_density + utf8_density {
            // Generate random UTF8 string
            let mut generated_string = String::with_capacity(str_len_chars);
            for _ in 0..str_len_chars {
                let char = corpus[rng_ref.random_range(0..corpus.len())];
                generated_string.push(char);
            }
            output_string_vec.push(Some(generated_string));
        } else {
            // Generate random ASCII-only string
            let value = rng_ref
                .sample_iter(&Alphanumeric)
                .take(str_len_chars)
                .collect();
            let value = String::from_utf8(value).unwrap();
            output_string_vec.push(Some(value));
        }
    }

    if is_string_view {
        let string_view_array: StringViewArray = output_string_vec.into_iter().collect();
        vec![ColumnarValue::Array(Arc::new(string_view_array))]
    } else {
        let string_array: StringArray = output_string_vec.clone().into_iter().collect();
        vec![ColumnarValue::Array(Arc::new(string_array))]
    }
}
