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

use datafusion_common::plan_err;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, ScalarUDFImpl, Signature, Volatility,
};

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
    fn invoke(
        &self,
        _args: &[ColumnarValue],
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
