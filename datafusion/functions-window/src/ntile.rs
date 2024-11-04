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

//! `ntile` window function implementation

use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use crate::utils::{
    get_scalar_value_from_args, get_signed_integer, get_unsigned_integer,
};
use datafusion_common::arrow::array::{ArrayRef, UInt64Array};
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::window_doc_sections::DOC_SECTION_RANKING;
use datafusion_expr::{
    Documentation, Expr, PartitionEvaluator, Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use field::WindowUDFFieldArgs;

get_or_init_udwf!(
    Ntile,
    ntile,
    "integer ranging from 1 to the argument value, dividing the partition as equally as possible"
);

pub fn ntile(arg: Expr) -> Expr {
    ntile_udwf().call(vec![arg])
}

#[derive(Debug)]
pub struct Ntile {
    signature: Signature,
}

impl Ntile {
    /// Create a new `ntile` function
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::UInt64,
                    DataType::UInt32,
                    DataType::UInt16,
                    DataType::UInt8,
                    DataType::Int64,
                    DataType::Int32,
                    DataType::Int16,
                    DataType::Int8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for Ntile {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_ntile_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_RANKING)
            .with_description(
                "Integer ranging from 1 to the argument value, dividing the partition as equally as possible",
            )
            .with_syntax_example("ntile(expression)")
            .with_argument("expression","An integer describing the number groups the partition should be split into")
            .build()
            .unwrap()
    })
}

impl WindowUDFImpl for Ntile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ntile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let scalar_n =
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 0)?
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "NTILE requires a positive integer".to_string(),
                    )
                })?;

        if scalar_n.is_null() {
            return exec_err!("NTILE requires a positive integer, but finds NULL");
        }

        if scalar_n.is_unsigned() {
            let n = get_unsigned_integer(scalar_n)?;
            Ok(Box::new(NtileEvaluator { n }))
        } else {
            let n: i64 = get_signed_integer(scalar_n)?;
            if n <= 0 {
                return exec_err!("NTILE requires a positive integer");
            }
            Ok(Box::new(NtileEvaluator { n: n as u64 }))
        }
    }
    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        let nullable = false;

        Ok(Field::new(field_args.name(), DataType::UInt64, nullable))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_ntile_doc())
    }
}

#[derive(Debug)]
struct NtileEvaluator {
    n: u64,
}

impl PartitionEvaluator for NtileEvaluator {
    fn evaluate_all(
        &mut self,
        _values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let num_rows = num_rows as u64;
        let mut vec: Vec<u64> = Vec::new();
        let n = u64::min(self.n, num_rows);
        for i in 0..num_rows {
            let res = i * n / num_rows;
            vec.push(res + 1)
        }
        Ok(Arc::new(UInt64Array::from(vec)))
    }
}
