use super::{LogicalType, NativeType};

#[derive(Debug)]
pub struct BuiltinType {
    native: NativeType,
}

impl LogicalType for BuiltinType {
    fn native(&self) -> &NativeType {
        &self.native
    }

    fn name(&self) -> Option<&str> {
        None
    }
}

impl From<NativeType> for BuiltinType {
    fn from(native: NativeType) -> Self {
        Self { native }
    }
}
