use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, FixedSizeBinaryArray,
    FixedSizeListArray, GenericByteArray, GenericListArray, OffsetSizeTrait,
    PrimitiveArray, RecordBatch, StructArray,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{ArrowNativeType, ByteArrayType};
use arrow::downcast_primitive_array;
use arrow_schema::DataType;

/// TODO - NEED TO MOVE THIS TO ARROW
///        this is needed as unlike `get_buffer_memory_size` or `get_array_memory_size`
///        or even `get_record_batch_memory_size` calling it on a batch before writing to spill and after reading it back
///        will return the same size (minus some optimization that IPC writer does for dictionaries)
///
pub trait GetActualSize {
    fn get_actually_used_size(&self) -> usize;
}

impl GetActualSize for RecordBatch {
    fn get_actually_used_size(&self) -> usize {
        self.columns()
            .iter()
            .map(|c| c.get_actually_used_size())
            .sum()
    }
}

pub trait GetActualSizeArray: Array {
    fn get_actually_used_size(&self) -> usize {
        self.get_buffer_memory_size()
    }
}

impl GetActualSize for dyn Array {
    fn get_actually_used_size(&self) -> usize {
        use arrow::array::AsArray;
        let array = self;

        // we can avoid this is we move this trait function to be in arrow
        downcast_primitive_array!(
            array => {
                array.get_actually_used_size()
            },
            DataType::Utf8 => {
                array.as_string::<i32>().get_actually_used_size()
            },
            DataType::LargeUtf8 => {
                array.as_string::<i64>().get_actually_used_size()
            },
            DataType::Binary => {
                array.as_binary::<i32>().get_actually_used_size()
            },
            DataType::LargeBinary => {
                array.as_binary::<i64>().get_actually_used_size()
            },
            DataType::FixedSizeBinary(_) => {
                array.as_fixed_size_binary().get_actually_used_size()
            },
            DataType::Struct(_) => {
                array.as_struct().get_actually_used_size()
            },
            DataType::List(_) => {
                array.as_list::<i32>().get_actually_used_size()
            },
            DataType::LargeList(_) => {
                array.as_list::<i64>().get_actually_used_size()
            },
            DataType::FixedSizeList(_, _) => {
                array.as_fixed_size_list().get_actually_used_size()
            },
            DataType::Boolean => {
                array.as_boolean().get_actually_used_size()
            },

            _ => {
                array.get_buffer_memory_size()
            }
        )
    }
}

impl GetActualSizeArray for ArrayRef {
    fn get_actually_used_size(&self) -> usize {
        self.as_ref().get_actually_used_size()
    }
}

impl GetActualSize for Option<&NullBuffer> {
    fn get_actually_used_size(&self) -> usize {
        self.map(|b| b.get_actually_used_size()).unwrap_or(0)
    }
}

impl GetActualSize for NullBuffer {
    fn get_actually_used_size(&self) -> usize {
        // len return in bits
        self.len() / 8
    }
}
impl GetActualSize for Buffer {
    fn get_actually_used_size(&self) -> usize {
        self.len()
    }
}

impl GetActualSize for BooleanBuffer {
    fn get_actually_used_size(&self) -> usize {
        // len return in bits
        self.len() / 8
    }
}

impl<O: ArrowNativeType> GetActualSize for OffsetBuffer<O> {
    fn get_actually_used_size(&self) -> usize {
        self.inner().inner().get_actually_used_size()
    }
}

impl GetActualSizeArray for BooleanArray {
    fn get_actually_used_size(&self) -> usize {
        let null_size = self.nulls().get_actually_used_size();
        let values_size = self.values().get_actually_used_size();
        null_size + values_size
    }
}

impl<T: ByteArrayType> GetActualSizeArray for GenericByteArray<T> {
    fn get_actually_used_size(&self) -> usize {
        let null_size = self.nulls().get_actually_used_size();
        let offsets_size = self.offsets().get_actually_used_size();

        let values_size = {
            let first_offset = self.value_offsets()[0].as_usize();
            let last_offset = self.value_offsets()[self.len()].as_usize();
            last_offset - first_offset
        };
        null_size + offsets_size + values_size
    }
}

impl GetActualSizeArray for FixedSizeBinaryArray {
    fn get_actually_used_size(&self) -> usize {
        let null_size = self.nulls().get_actually_used_size();
        let values_size = self.value_length() as usize * self.len();

        null_size + values_size
    }
}

impl<T: ArrowPrimitiveType> GetActualSizeArray for PrimitiveArray<T> {
    fn get_actually_used_size(&self) -> usize {
        let null_size = self.nulls().get_actually_used_size();
        let values_size = self.values().inner().get_actually_used_size();

        null_size + values_size
    }
}

impl<O: OffsetSizeTrait> GetActualSizeArray for GenericListArray<O> {
    fn get_actually_used_size(&self) -> usize {
        let null_size = self.nulls().get_actually_used_size();
        let offsets_size = self.offsets().get_actually_used_size();

        let values_size = {
            let first_offset = self.value_offsets()[0].as_usize();
            let last_offset = self.value_offsets()[self.len()].as_usize();

            self.values()
                .slice(first_offset, last_offset - first_offset)
                .get_actually_used_size()
        };
        null_size + offsets_size + values_size
    }
}

impl GetActualSizeArray for FixedSizeListArray {
    fn get_actually_used_size(&self) -> usize {
        let null_size = self.nulls().get_actually_used_size();
        let values_size = self.values().get_actually_used_size();

        null_size + values_size
    }
}

impl GetActualSizeArray for StructArray {
    fn get_actually_used_size(&self) -> usize {
        let null_size = self.nulls().get_actually_used_size();
        let values_size = self
            .columns()
            .iter()
            .map(|array| array.get_actually_used_size())
            .sum::<usize>();

        null_size + values_size
    }
}

// TODO - need to add to more arrays
