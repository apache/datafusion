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

use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, OnceLock};

use crate::utils::{
    get_scalar_value_from_args, get_signed_integer, get_unsigned_integer,
};
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::arrow::array::UInt64Array;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::arrow::datatypes::Field;
use datafusion_common::{
    arrow_datafusion_err, exec_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::window_doc_sections::DOC_SECTION_RANKING;
use datafusion_expr::{
    Documentation, PartitionEvaluator, Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::expr::ExpressionArgs;
use datafusion_functions_window_common::field;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use field::WindowUDFFieldArgs;

get_or_init_udwf!(
    Ntile,
    ntile,
    "integer ranging from 1 to the argument value, dividing the partition as equally as possible"
);

pub fn ntile(arg: datafusion_expr::Expr) -> datafusion_expr::Expr {
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
    /// Create a new `ntile` function
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
// TODO JP look into data types
// TODO JP delete the ntile file
// TODO JP if n is not possitive send error

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

    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        parse_expr(expr_args.input_exprs(), expr_args.input_types())
            .into_iter()
            .collect::<Vec<_>>()
    }

    // TODO JP  will look into this
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

        let data_type = partition_evaluator_args
            .input_types()
            .get(0)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "NTILE requires a positive integer".to_string(),
                )
            })?
            .to_owned();

        let n = get_unsigned_integer(scalar_n)?;

        let data_type = partition_evaluator_args
            .input_types()
            .get(0)
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "NTILE requires a positive integer".to_string(),
                )
            })?
            .to_owned();

        Ok(Box::new(NtileEvaluator { n, data_type }))
    }
    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<Field> {
        let nullable = false;
        Ok(Field::new(self.name(), DataType::UInt64, nullable))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_ntile_doc())
    }
}

#[derive(Debug)]
struct NtileEvaluator {
    n: u64,
    /// Output data type
    data_type: DataType,
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

fn parse_expr(
    input_exprs: &[Arc<dyn PhysicalExpr>],
    input_types: &[DataType],
) -> Result<Arc<dyn PhysicalExpr>> {
    assert!(!input_exprs.is_empty());
    assert!(!input_types.is_empty());

    let n = get_scalar_value_from_args(input_exprs, 0)?.ok_or_else(|| {
        DataFusionError::Execution("NTILE requires a positive integer".to_string())
    })?;

    if n.is_null() {
        return exec_err!("NTILE requires a positive integer, but finds NULL");
    }

    if n.is_unsigned() {
        let n = get_unsigned_integer(n)?;
        Ok(
            Arc::new(datafusion_physical_expr::expressions::Literal::new(
                n.into(),
            )) as Arc<dyn PhysicalExpr>,
        )
    } else {
        let n: i64 = get_signed_integer(n)?;
        if n <= 0 {
            return exec_err!("NTILE requires a positive integer");
        }

        Ok(
            Arc::new(datafusion_physical_expr::expressions::Literal::new(
                n.into(),
            )) as Arc<dyn PhysicalExpr>,
        )
    }
}
