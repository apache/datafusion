use std::{
    ffi::{c_char, c_int, c_void, CString},
    ptr::addr_of,
};

use arrow::{
    array::StructArray,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::FFI_ArrowArrayStream,
};
use arrow::{
    array::{Array, RecordBatch, RecordBatchReader},
    ffi_stream::ArrowArrayStreamReader,
};
use datafusion::error::Result;
use datafusion::{
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::{executor::block_on, Stream, TryStreamExt};

pub fn record_batch_to_arrow_stream(stream: SendableRecordBatchStream) -> FFI_ArrowArrayStream {
    let private_data = Box::new(RecoredBatchStreamPrivateData {
        stream,
        last_error: None,
    });

    FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release_stream),
        private_data: Box::into_raw(private_data) as *mut c_void,
    }
}

struct RecoredBatchStreamPrivateData {
    stream: SendableRecordBatchStream,
    last_error: Option<CString>,
}

// callback used to drop [FFI_ArrowArrayStream] when it is exported.
unsafe extern "C" fn release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let stream = &mut *stream;

    stream.get_schema = None;
    stream.get_next = None;
    stream.get_last_error = None;

    let private_data = Box::from_raw(stream.private_data as *mut RecoredBatchStreamPrivateData);
    drop(private_data);

    stream.release = None;
}

// The callback used to get array schema
unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    schema: *mut FFI_ArrowSchema,
) -> c_int {
    ExportedRecordBatchStream { stream }.get_schema(schema)
}

// The callback used to get next array
unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    array: *mut FFI_ArrowArray,
) -> c_int {
    ExportedRecordBatchStream { stream }.get_next(array)
}

// The callback used to get the error from last operation on the `FFI_ArrowArrayStream`
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let mut ffi_stream = ExportedRecordBatchStream { stream };
    // The consumer should not take ownership of this string, we should return
    // a const pointer to it.
    match ffi_stream.get_last_error() {
        Some(err_string) => err_string.as_ptr(),
        None => std::ptr::null(),
    }
}

struct ExportedRecordBatchStream {
    stream: *mut FFI_ArrowArrayStream,
}

impl ExportedRecordBatchStream {
    fn get_private_data(&mut self) -> &mut RecoredBatchStreamPrivateData {
        unsafe { &mut *((*self.stream).private_data as *mut RecoredBatchStreamPrivateData) }
    }

    pub fn get_schema(&mut self, out: *mut FFI_ArrowSchema) -> i32 {
        let private_data = self.get_private_data();
        let stream = &private_data.stream;

        let schema = FFI_ArrowSchema::try_from(stream.schema().as_ref());

        match schema {
            Ok(schema) => {
                unsafe { std::ptr::copy(addr_of!(schema), out, 1) };
                std::mem::forget(schema);
                0
            }
            Err(ref err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                1
            }
        }
    }

    pub fn get_next(&mut self, out: *mut FFI_ArrowArray) -> i32 {
        let private_data = self.get_private_data();

        let maybe_batch = block_on(private_data.stream.try_next());

        match maybe_batch {
            Ok(None) => {
                // Marks ArrowArray released to indicate reaching the end of stream.
                unsafe { std::ptr::write(out, FFI_ArrowArray::empty()) }
                0
            }
            Ok(Some(batch)) => {
                let struct_array = StructArray::from(batch);
                let array = FFI_ArrowArray::new(&struct_array.to_data());

                unsafe { std::ptr::write_unaligned(out, array) };
                0
            }
            Err(err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                1
            }
        }
    }

    pub fn get_last_error(&mut self) -> Option<&CString> {
        self.get_private_data().last_error.as_ref()
    }
}

pub struct ConsumerRecordBatchStream {
    reader: ArrowArrayStreamReader,
}

impl TryFrom<FFI_ArrowArrayStream> for ConsumerRecordBatchStream {
    type Error = DataFusionError;

    fn try_from(value: FFI_ArrowArrayStream) -> std::result::Result<Self, Self::Error> {
        let reader = ArrowArrayStreamReader::try_new(value)?;

        Ok(Self { reader })
    }
}

impl RecordBatchStream for ConsumerRecordBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.reader.schema()
    }
}

impl Stream for ConsumerRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let batch = self
            .reader
            .next()
            .map(|v| v.map_err(|e| DataFusionError::ArrowError(e, None)));

        std::task::Poll::Ready(batch)
    }
}
