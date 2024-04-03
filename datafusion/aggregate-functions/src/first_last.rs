
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

use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::format_state_name;
use datafusion_common::Result;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::{Accumulator, AccumulatorFactoryFunction, AggregateUDF, AggregateUDFImpl, Signature, Volatility};
use std::any::Any;
use std::fmt::Debug;

make_udf_function!(
    FirstValue,
    first_value,
    value: Expr,
    "Returns the first value in a group of values.",
    first_value_fn
);


pub struct FirstValue {
    signature: Signature,
    aliases: Vec<String>,
    accumulator: AccumulatorFactoryFunction,
}

impl Debug for FirstValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("FirstValue")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl FirstValue {
    pub fn new(
        accumulator: AccumulatorFactoryFunction,
    ) -> Self {
        Self {
            aliases: vec![
                String::from("FIRST_VALUE"),
            ],
            signature: Signature::uniform(1, NUMERICS.to_vec(), Volatility::Immutable),
            accumulator,
        }
    }
}

impl AggregateUDFImpl for FirstValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "FIRST_VALUE"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        (self.accumulator)(acc_args)
    }

    fn state_fields(
        &self,
        name: &str,
        value_type: DataType,
        ordering_fields: Vec<Field>,
    ) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(name, "first_value"),
            value_type,
            true,
        )];
        fields.extend(ordering_fields);
        fields.push(Field::new("is_set", DataType::Boolean, true));
        Ok(fields)
    }
}

/// Creates a new UDAF with a specific signature, state type and return type.
/// The signature and state type must match the `Accumulator's implementation`.
/// TOOD: We plan to move aggregate function to its own crate. This function will be deprecated then.
pub fn create_first_value(
    name: &str,
    signature: Signature,
    accumulator: AccumulatorFactoryFunction,
) -> AggregateUDF {
    AggregateUDF::from(FirstValue::new(accumulator))
}