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

//! UDF support

use super::type_coercion::coerce;
use crate::error::Result;
use crate::physical_plan::PhysicalExpr;
use arrow::datatypes::{DataType, Schema};
use std::any::Any;
use std::fmt;

pub use datafusion_expr::ScalarUDF;

use arrow::record_batch::RecordBatch;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// Create a physical expression of the UDF.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDF.
pub fn create_physical_expr(
    fun: &ScalarUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    // coerce
    let coerced_phy_exprs = coerce(input_phy_exprs, input_schema, &fun.signature)?;

    let coerced_exprs_types = coerced_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(ScalarUDFExpr {
        fun: fun.clone(),
        name: fun.name.clone(),
        args: coerced_phy_exprs.clone(),
        return_type: (fun.return_type)(&coerced_exprs_types)?.as_ref().clone(),
    }))
}

/// Physical expression of a UDF.
#[derive(Debug)]
pub struct ScalarUDFExpr {
    fun: ScalarUDF,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl ScalarUDFExpr {
    /// create a ScalarUDFExpr
    pub fn new(
        name: &str,
        fun: ScalarUDF,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: &DataType,
    ) -> Self {
        Self {
            fun,
            name: name.to_string(),
            args,
            return_type: return_type.clone(),
        }
    }

    /// return fun
    pub fn fun(&self) -> &ScalarUDF {
        &self.fun
    }

    /// return name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// return args
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }
}

impl fmt::Display for ScalarUDFExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl PhysicalExpr for ScalarUDFExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // evaluate the arguments, if there are no arguments we'll instead pass in a null array
        // indicating the batch size (as a convention)
        // TODO need support zero input arguments
        let inputs = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        // evaluate the function
        let fun = self.fun.fun.as_ref();
        (fun)(&inputs)
    }
}
