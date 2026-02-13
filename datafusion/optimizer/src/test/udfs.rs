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

use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, Expr, ExpressionPlacement, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature,
};

/// A configurable test UDF for optimizer tests.
/// Defaults to `MoveTowardsLeafNodes` placement. Use `with_placement()` to override.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PlacementTestUDF {
    signature: Signature,
    placement: ExpressionPlacement,
    id: usize,
}

impl Default for PlacementTestUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl PlacementTestUDF {
    pub fn new() -> Self {
        Self {
            // Accept any one or two arguments and return UInt32 for testing purposes.
            // The actual types don't matter since this UDF is not intended for execution.
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]),
                datafusion_expr::Volatility::Immutable,
            ),
            placement: ExpressionPlacement::MoveTowardsLeafNodes,
            id: 0,
        }
    }

    /// Set the expression placement for this UDF, which is used by optimizer rules to determine where in the plan the expression should be placed.
    /// This also resets the name of the UDF to a default based on the placement.
    pub fn with_placement(mut self, placement: ExpressionPlacement) -> Self {
        self.placement = placement;
        self
    }

    /// Set the id of the UDF.
    /// This is an arbitrary made up field to allow creating multiple distinct UDFs with the same placement.
    pub fn with_id(mut self, id: usize) -> Self {
        self.id = id;
        self
    }
}

impl ScalarUDFImpl for PlacementTestUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        match self.placement {
            ExpressionPlacement::MoveTowardsLeafNodes => "leaf_udf",
            ExpressionPlacement::KeepInPlace => "keep_in_place_udf",
            ExpressionPlacement::Column => "column_udf",
            ExpressionPlacement::Literal => "literal_udf",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("PlacementTestUDF: not intended for execution")
    }
    fn placement(&self, _args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.placement
    }
}

/// Create a `leaf_udf(arg)` expression with `MoveTowardsLeafNodes` placement.
pub fn leaf_udf_expr(arg: Expr) -> Expr {
    let udf = ScalarUDF::new_from_impl(
        PlacementTestUDF::new().with_placement(ExpressionPlacement::MoveTowardsLeafNodes),
    );
    udf.call(vec![arg])
}
