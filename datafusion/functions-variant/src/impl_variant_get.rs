macro_rules! impl_variant_get_typed {
    (
        $(#[$meta:meta])*
        $struct_name:ident,
        $func_name:expr,
        $return_type:expr,
        $scalar_from:expr,
        $array_from:expr,
        $extract:expr $(,)?
    ) => {
        $(#[$meta])*
        #[derive(Debug, Hash, PartialEq, Eq)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    signature: Signature::new(TypeSignature::Any(2), Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $struct_name {

            fn name(&self) -> &str {
                $func_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok($return_type)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                invoke_variant_get_typed(&args, $scalar_from, $array_from, $extract)
            }
        }
    };
}

pub(crate) use impl_variant_get_typed;
