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

use std::sync::Arc;

use arrow::array::StructArray;
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use parquet_variant::{Variant, VariantBuilder};
use parquet_variant_compute::{VariantArray, VariantType};

use crate::shared::{ensure, try_parse_variant_scalar};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct VariantListConstruct {
    signature: Signature,
}

impl Default for VariantListConstruct {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VariantListConstruct {
    fn name(&self) -> &str {
        "variant_list_construct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "implemented return_field_from_args instead".into(),
        ))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("metadata", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, false),
        ]));

        Ok(Arc::new(
            Field::new(self.name(), data_type, true).with_extension_type(VariantType),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // validate arguments
        let argument_fields = args.arg_fields;
        let argument_values = args.args;

        // note: arguments must be nonempty
        // this is required by the ListBuilder
        ensure(
            !argument_values.is_empty(),
            "expected nonempty list of arguments",
        )?;

        ensure(
            argument_fields.len() == argument_values.len(),
            "argument fields and values must be of same length",
        )?;

        let all_variant_fields = argument_fields
            .iter()
            .all(|f| matches!(f.extension_type(), VariantType));

        ensure(
            all_variant_fields,
            "argument fields must all have the VariantType extension",
        )?;

        let all_arguments_scalar = argument_values
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)));

        ensure(
            all_arguments_scalar,
            "todo: how do you construct lists of lists?",
        )?;

        let mut v = VariantBuilder::new();
        let mut l = v.new_list();

        // note: it would be nice to reserve capacity
        // something like: v.new_list_with_capacity(usize)
        // or ListBuilder().with_capacity(usize)
        // or l.reserve(usize)

        for v in argument_values {
            let ColumnarValue::Scalar(sv) = v else {
                unreachable!()
            };

            let variant = try_parse_variant_scalar(&sv)?;
            let v = variant.value(0);

            l.append_value(v);
        }

        l.finish();

        let (m, v) = v.finish();

        let v = Variant::new(m.as_ref(), v.as_ref());

        let out: StructArray = VariantArray::from_iter([v]).into();

        Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(out))))
    }
}
