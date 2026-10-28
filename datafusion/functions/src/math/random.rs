// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Float64;
use rand::{Rng, rng};

use datafusion_common::{Result, assert_or_internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = r#"Returns a random float value in the range [0, 1).
The random seed is unique to each row."#,
    syntax_example = "random()",
    sql_example = r#"```sql
> SELECT random();
+------------------+
| random()         |
+------------------+
| 0.7389238902938  |
+------------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RandomFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for RandomFunc {
    fn default() -> Self {
        RandomFunc::new()
    }
}

impl RandomFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
            aliases: vec![String::from("rand")],
        }
    }
}

impl ScalarUDFImpl for RandomFunc {
    fn name(&self) -> &str {
        "random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        assert_or_internal_err!(
            args.args.is_empty(),
            "{} function does not accept arguments",
            self.name()
        );
        let mut rng = rng();
        let mut values = vec![0.0; args.number_rows];
        // Equivalent to set each element with rng.random_range(0.0..1.0), but more efficient
        rng.fill(&mut values[..]);
        let array = Float64Array::from(values);

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
