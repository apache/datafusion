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
use std::any::Any;
use std::fmt::Debug;

use datafusion_common::plan_err;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, PartitionEvaluator, ScalarUDFImpl,
    Signature, Volatility, WindowUDFImpl,
};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;

mod roundtrip_logical_plan;
mod roundtrip_physical_plan;
mod serialize;

#[derive(Debug, PartialEq, Eq, Hash)]
struct MyRegexUdf {
    signature: Signature,
    // regex as original string
    pattern: String,
    aliases: Vec<String>,
}

impl MyRegexUdf {
    fn new(pattern: String) -> Self {
        let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);
        Self {
            signature,
            pattern,
            aliases: vec!["aggregate_udf_alias".to_string()],
        }
    }
}

/// Implement the ScalarUDFImpl trait for MyRegexUdf
impl ScalarUDFImpl for MyRegexUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regex_udf"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, args: &[DataType]) -> datafusion_common::Result<DataType> {
        if matches!(args, [DataType::Utf8]) {
            Ok(DataType::Int64)
        } else {
            plan_err!("regex_udf only accepts Utf8 arguments")
        }
    }
    fn invoke_batch(
        &self,
        _args: &[ColumnarValue],
        _number_rows: usize,
    ) -> datafusion_common::Result<ColumnarValue> {
        unimplemented!()
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MyRegexUdfNode {
    #[prost(string, tag = "1")]
    pub pattern: String,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MyAggregateUDF {
    signature: Signature,
    result: String,
}

impl MyAggregateUDF {
    fn new(result: String) -> Self {
        let signature = Signature::exact(vec![DataType::Int64], Volatility::Immutable);
        Self { signature, result }
    }
}

impl AggregateUDFImpl for MyAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "aggregate_udf"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn accumulator(
        &self,
        _acc_args: AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        unimplemented!()
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MyAggregateUdfNode {
    #[prost(string, tag = "1")]
    pub result: String,
}

#[derive(Debug)]
pub(in crate::cases) struct CustomUDWF {
    signature: Signature,
    payload: String,
}

impl CustomUDWF {
    pub fn new(payload: String) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            payload,
        }
    }
}

impl WindowUDFImpl for CustomUDWF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "custom_udwf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> datafusion_common::Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CustomUDWFEvaluator {}))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> datafusion_common::Result<Field> {
        Ok(Field::new(field_args.name(), DataType::UInt64, false))
    }
}

#[derive(Debug)]
struct CustomUDWFEvaluator;

impl PartitionEvaluator for CustomUDWFEvaluator {}

#[derive(Clone, PartialEq, ::prost::Message)]
pub(in crate::cases) struct CustomUDWFNode {
    #[prost(string, tag = "1")]
    pub payload: String,
}
