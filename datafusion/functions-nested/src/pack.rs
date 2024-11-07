use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::error::Result as DFResult;
use datafusion_common::{exec_err, plan_err, ExprSchema};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::ExprSchemable;
use datafusion_expr::{
    ColumnarValue, Expr, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use itertools::Itertools;

#[derive(Debug)]
pub struct Pack {
    signature: Signature,
    names: Vec<String>,
}

impl Pack {
    pub(crate) const NAME: &'static str = "struct.pack";

    pub fn new<I>(names: I) -> Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(0), TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
            names: names
                .into_iter()
                .map(|n| n.as_ref().to_string())
                .collect_vec(),
        }
    }

    pub fn names(&self) -> &[String] {
        self.names.as_slice()
    }

    pub fn new_instance<T>(
        names: impl IntoIterator<Item = T>,
        args: impl IntoIterator<Item = Expr>,
    ) -> Expr
    where
        T: AsRef<str>,
    {
        Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(Pack::new(
                names
                    .into_iter()
                    .map(|n| n.as_ref().to_string())
                    .collect_vec(),
            ))),
            args: args.into_iter().collect_vec(),
        })
    }

    pub fn new_instance_from_pair(
        pairs: impl IntoIterator<Item = (impl AsRef<str>, Expr)>,
    ) -> Expr {
        let (names, args): (Vec<String>, Vec<Expr>) = pairs
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), v))
            .unzip();
        Expr::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(Pack::new(names))),
            args,
        })
    }
}

impl ScalarUDFImpl for Pack {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        todo!()
    }

    // fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
    //     if self.names.len() != arg_types.len() {
    //         return plan_err!("The number of arguments provided argument must equal the number of expected field names");
    //     }
    //
    //     let fields = self
    //         .names
    //         .iter()
    //         .zip(arg_types.iter())
    //         This is how ee currently set nullability
    //         .map(|(name, dt)| Field::new(name, dt.clone(), true))
    //         .collect::<Fields>();
    //
    //     Ok(DataType::Struct(fields))
    // }

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> DFResult<ColumnarValue> {
        if number_rows == 0 {
            return Ok(ColumnarValue::Array(Arc::new(
                StructArray::new_empty_fields(number_rows, None),
            )))
        }

        if self.names.len() != args.len() {
            return exec_err!("The number of arguments provided argument must equal the number of expected field names");
        }

        let children = self
            .names
            .iter()
            .zip(args.iter())
            .map(|(name, arg)| {
                let arr = match arg {
                    ColumnarValue::Array(array_value) => array_value.clone(),
                    ColumnarValue::Scalar(scalar_value) => scalar_value.to_array()?,
                };

                Ok((name.as_str(), arr))
            })
            .collect::<DFResult<Vec<_>>>()?;

        let (fields, arrays): (Vec<_>, _) = children
            .into_iter()
            // Here I can either set nullability as true or dependent on the presence of nulls in the array,
            // both are not correct nullability is dependent on the schema and not a chunk of the data
            .map(|(name, array)| {
                (Field::new(name, array.data_type().clone(), true), array)
            })
            .unzip();

        let struct_array = StructArray::try_new(fields.into(), arrays, None)?;

        Ok(ColumnarValue::from(Arc::new(struct_array) as ArrayRef))
    }

    // TODO(joe): support propagating nullability into invoke and therefore use the below method
    // see https://github.com/apache/datafusion/issues/12819
    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        _arg_types: &[DataType],
    ) -> DFResult<DataType> {
        if self.names.len() != args.len() {
            return plan_err!("The number of arguments provided argument must equal the number of expected field names");
        }

        let fields = self
            .names
            .iter()
            .zip(args.iter())
            .map(|(name, expr)| {
                let (dt, null) = expr.data_type_and_nullable(schema)?;
                Ok(Field::new(name, dt, null))
            })
            .collect::<DFResult<Vec<Field>>>()?;

        Ok(DataType::Struct(Fields::from(fields)))
    }

    fn invoke_batch_with_return_type(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
        return_type: &DataType,
    ) -> DFResult<ColumnarValue> {
        if self.names.len() != args.len() {
            return exec_err!("The number of arguments provided argument must equal the number of expected field names");
        }

        let fields = match return_type {
            DataType::Struct(fields) => fields.clone(),
            _ => {
                return exec_err!(
                    "Return type must be a struct, however it was {:?}",
                    return_type
                )
            }
        };

        let children = fields
            .into_iter()
            .zip(args.iter())
            .map(|(name, arg)| {
                let arr = match arg {
                    ColumnarValue::Array(array_value) => array_value.clone(),
                    ColumnarValue::Scalar(scalar_value) => scalar_value.to_array()?,
                };

                Ok((name.clone(), arr))
            })
            .collect::<DFResult<Vec<_>>>()?;

        let struct_array = StructArray::from(children);

        Ok(ColumnarValue::from(Arc::new(struct_array) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::pack::Pack;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow_array::Array;
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field, Fields};
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, ColumnarValue, ScalarUDFImpl};

    #[test]
    fn test_pack_not_null() {
        let a1 = Arc::new(Int32Array::from_iter_values_with_nulls(
            vec![1, 2],
            Some(NullBuffer::from([true, false].as_slice())),
        )) as ArrayRef;
        let schema = DFSchema::from_unqualified_fields(
            Fields::from([Arc::new(Field::new("a", DataType::Int32, true))].as_slice()),
            HashMap::new(),
        );
        let pack = Pack::new(vec!["a"]);

        assert_eq!(
            DataType::Struct(Fields::from([Arc::new(Field::new(
                "a",
                DataType::Int32,
                true
            ))])),
            pack.invoke_batch(&[ColumnarValue::Array(a1.clone())], a1.len())
                .unwrap()
                .data_type()
        );
    }

    // Cannot have a return value of struct[("a", int32, null)], since the nullability is static
    #[test]
    // fails
    fn test_pack_null() {
        let a1 = Arc::new(Int32Array::from_iter_values(vec![1, 2]));
        let schema = DFSchema::from_unqualified_fields(
            Fields::from([Arc::new(Field::new("a", DataType::Int32, false))].as_slice()),
            HashMap::new(),
        );
        let pack = Pack::new(vec!["a"]);

        assert_eq!(
            DataType::Struct(Fields::from([Arc::new(Field::new(
                "a",
                DataType::Int32,
                false
            ))])),
            pack.invoke_batch(&[ColumnarValue::Array(a1.clone())], a1.len())
                .unwrap()
                .data_type()
        );
    }

    #[test]
    fn test_pack_rt_null() {
        let a1 = Arc::new(Int32Array::from_iter_values(vec![1, 2])) as ArrayRef;
        let schema = DFSchema::from_unqualified_fields(
            Fields::from([Arc::new(Field::new("a", DataType::Int32, true))]),
            HashMap::new(),
        )
        .unwrap();
        let pack = Pack::new(vec!["a"]);

        let rt = pack
            .return_type_from_exprs(&[col("a")], &schema, &[DataType::Int32])
            .unwrap();

        let ret = pack
            .invoke_batch_with_return_type(&[ColumnarValue::Array(a1.clone())], a1.len(), &rt)
            .unwrap();

        println!("{:?}", ret.into_array(1).unwrap().data_type());
    }

    #[test]
    fn test_pack_rt_not_null() {
        let a1 = Arc::new(Int32Array::from_iter_values(vec![1, 2])) as ArrayRef;
        let schema = DFSchema::from_unqualified_fields(
            Fields::from([Arc::new(Field::new("a", DataType::Int32, false))]),
            HashMap::new(),
        )
        .unwrap();
        let pack = Pack::new(vec!["a"]);

        let rt = pack
            .return_type_from_exprs(&[col("a")], &schema, &[DataType::Int32])
            .unwrap();

        let ret = pack
            .invoke_batch_with_return_type(&[ColumnarValue::Array(a1.clone())], a1.len(), &rt)
            .unwrap();

        println!("{:?}", ret.into_array(1).unwrap().data_type());
    }
}
