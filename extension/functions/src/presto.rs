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

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{
        ArrayRef, Int64Array, Time32MillisecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray,
    },
    datatypes::{DataType, TimeUnit},
};
use chrono::Timelike;
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use datafusion_expr::{
    ReturnTypeFunction, ScalarFunctionDef, ScalarFunctionPackage, Signature, Volatility,
};

#[derive(Debug)]
pub struct CurrentTimeFunction;

impl ScalarFunctionDef for CurrentTimeFunction {
    fn name(&self) -> &str {
        "current_time"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        let return_type = Arc::new(DataType::Time32(TimeUnit::Millisecond));
        Arc::new(move |_| Ok(return_type.clone()))
    }

    fn execute(&self, _args: &[ArrayRef]) -> Result<ArrayRef> {
        let current_time = chrono::Utc::now().time();
        let milliseconds_since_midnight = current_time.num_seconds_from_midnight() * 1000;
        let array =
            Time32MillisecondArray::from(vec![Some(milliseconds_since_midnight as i32)]);
        Ok(Arc::new(array) as ArrayRef)
    }
}
#[derive(Debug)]
pub struct CurrentTimestampFunction;

impl ScalarFunctionDef for CurrentTimestampFunction {
    fn name(&self) -> &str {
        "current_timestamp"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Microsecond, None))))
    }

    fn execute(&self, _args: &[ArrayRef]) -> Result<ArrayRef> {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let array = TimestampMillisecondArray::from(vec![Some(n.as_millis() as i64)]);
        Ok(Arc::new(array) as ArrayRef)
    }
}
#[derive(Debug)]
pub struct CurrentTimestampPFunction;

impl ScalarFunctionDef for CurrentTimestampPFunction {
    fn name(&self) -> &str {
        "current_timestamp_p"
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![DataType::Int64], Volatility::Immutable)
    }

    fn return_type(&self) -> ReturnTypeFunction {
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))))
    }

    fn execute(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let precision = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0) as usize;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| DataFusionError::Execution(err.to_string()))?;
        let nanos = now.as_nanos() as i64;

        let adjusted_nanos = if precision > 9 {
            nanos
        } else {
            let factor = 10_i64.pow(9 - precision as u32);
            (nanos / factor) * factor
        };

        let array = TimestampNanosecondArray::from(vec![Some(adjusted_nanos)]);
        Ok(Arc::new(array) as ArrayRef)
    }
}

// Function package declaration
pub struct FunctionPackage;

impl ScalarFunctionPackage for FunctionPackage {
    fn functions(&self) -> Vec<Box<dyn ScalarFunctionDef>> {
        vec![
            Box::new(CurrentTimeFunction),
            Box::new(CurrentTimestampFunction),
            Box::new(CurrentTimestampPFunction),
        ]
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::array::{
        ArrayRef, Int64Array, TimestampMillisecondArray, TimestampNanosecondArray,
    };
    use chrono::Utc;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::ScalarFunctionDef;
    use tokio;

    use crate::{
        presto::{CurrentTimestampFunction, CurrentTimestampPFunction},
        utils::{execute, test_expression},
    };

    use super::FunctionPackage;

    #[tokio::test]
    async fn test_current_time() -> Result<()> {
        let current = Utc::now();
        let formatted = current.format("%H:%M:%S").to_string();
        test_expression!("current_time()", formatted);
        Ok(())
    }

    #[tokio::test]
    async fn test_current_timestamp() -> Result<()> {
        let current_timestamp_function = CurrentTimestampFunction {};
        let result = current_timestamp_function.execute(&[]).unwrap();
        let result_array = result
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!((result_array - now).abs() <= 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_current_timestamp_p() -> Result<()> {
        let function = CurrentTimestampPFunction {};
        let precision_array = Int64Array::from(vec![9]);
        let args = vec![Arc::new(precision_array) as ArrayRef];
        let result = function.execute(&args).unwrap();
        let result_array = result
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        assert!((result_array - now).abs() <= 1_000_000_000);
        Ok(())
    }
}
