use arrow_schema::DataType;

pub trait LogicallyEq<Rhs: ?Sized = Self> {
    #[must_use]
    fn logically_eq(&self, other: &Rhs) -> bool;
}

impl LogicallyEq for DataType {
    fn logically_eq(&self, other: &Self) -> bool {
        use DataType::*;

        match (self, other) {
            (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
            | (Binary | LargeBinary | BinaryView, Binary | LargeBinary | BinaryView) => {
                true
            }
            (Dictionary(_, inner), other) | (other, Dictionary(_, inner)) => {
                other.logically_eq(inner)
            }
            (RunEndEncoded(_, inner), other) | (other, RunEndEncoded(_, inner)) => {
                other.logically_eq(inner.data_type())
            }
            _ => self == other,
        }
    }
}
