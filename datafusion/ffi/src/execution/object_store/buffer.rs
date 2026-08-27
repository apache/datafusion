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

//! FFI-safe transfer of [`Bytes`] buffers.

use std::ffi::c_void;

use bytes::Bytes;

/// A stable struct for sharing a byte buffer across FFI boundaries.
///
/// The buffer is *borrowed* from the producing library for the lifetime of this
/// struct: `private_data` owns a boxed [`Bytes`] and `ptr` points into it.
/// Dropping this struct calls back into the producing library to release that
/// allocation, so the bytes are never freed by the wrong allocator.
///
/// Because the memory stays valid until release, the consuming side can build a
/// [`Bytes`] that borrows it directly with [`Bytes::from_owner`] rather than
/// copying. This matters on the read path, where every scanned byte of every
/// data file crosses this struct.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_Bytes {
    /// Pointer to the start of the buffer. Valid for `len` bytes until
    /// `release` is called. Never null, even for an empty buffer.
    pub ptr: *const u8,

    /// Length of the buffer in bytes.
    pub len: u64,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the
    /// buffer. The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

// Safety: the buffer is immutable and owned by the boxed `Bytes` in
// `private_data`, which is itself `Send + Sync`. Access is read-only.
unsafe impl Send for FFI_Bytes {}
unsafe impl Sync for FFI_Bytes {}

unsafe extern "C" fn release_fn_wrapper(buffer: &mut FFI_Bytes) {
    unsafe {
        debug_assert!(!buffer.private_data.is_null());
        drop(Box::from_raw(buffer.private_data as *mut Bytes));
        buffer.private_data = std::ptr::null_mut();
        buffer.ptr = std::ptr::NonNull::dangling().as_ptr();
        buffer.len = 0;
    }
}

impl From<Bytes> for FFI_Bytes {
    fn from(bytes: Bytes) -> Self {
        let len = bytes.len() as u64;
        let boxed = Box::new(bytes);
        // Take the pointer from the boxed value so it remains valid after the
        // move into the box.
        let ptr = if boxed.is_empty() {
            // `Bytes::as_ptr` on an empty buffer may be dangling but is still
            // required to be non-null for `slice::from_raw_parts`.
            std::ptr::NonNull::dangling().as_ptr()
        } else {
            boxed.as_ptr()
        };

        Self {
            ptr,
            len,
            release: release_fn_wrapper,
            private_data: Box::into_raw(boxed) as *mut c_void,
        }
    }
}

impl AsRef<[u8]> for FFI_Bytes {
    fn as_ref(&self) -> &[u8] {
        // Safety: `ptr` is non-null and valid for `len` bytes until `release`,
        // which only runs in `Drop`.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

impl From<FFI_Bytes> for Bytes {
    fn from(buffer: FFI_Bytes) -> Self {
        let release_ptr = buffer.release as *const ();
        if std::ptr::eq(release_ptr, release_fn_wrapper as *const ()) {
            // Same library: unwrap the original `Bytes` and keep its refcount
            // rather than adding an owner indirection.
            let mut buffer = std::mem::ManuallyDrop::new(buffer);
            let bytes = unsafe { *Box::from_raw(buffer.private_data as *mut Bytes) };
            buffer.private_data = std::ptr::null_mut();
            return bytes;
        }

        // Foreign library: borrow the buffer in place. The `FFI_Bytes` is kept
        // alive by the resulting `Bytes` and released when its refcount drops.
        Bytes::from_owner(buffer)
    }
}

impl Drop for FFI_Bytes {
    fn drop(&mut self) {
        if !self.private_data.is_null() {
            unsafe { (self.release)(self) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_round_trip() {
        let original = Bytes::from_static(b"hello world");
        let ffi = FFI_Bytes::from(original.clone());
        assert_eq!(ffi.len, 11);
        assert_eq!(ffi.as_ref(), b"hello world");

        let restored: Bytes = ffi.into();
        assert_eq!(restored, original);
    }

    #[test]
    fn empty_bytes_round_trip() {
        let ffi = FFI_Bytes::from(Bytes::new());
        assert_eq!(ffi.len, 0);
        assert!(!ffi.ptr.is_null());
        assert_eq!(ffi.as_ref(), b"");

        let restored: Bytes = ffi.into();
        assert!(restored.is_empty());
    }

    /// Simulate the foreign path, where the release function pointer belongs to
    /// another library and the buffer must be borrowed rather than unwrapped.
    #[test]
    fn foreign_bytes_borrow_round_trip() {
        unsafe extern "C" fn foreign_release(buffer: &mut FFI_Bytes) {
            unsafe {
                drop(Box::from_raw(buffer.private_data as *mut Bytes));
                buffer.private_data = std::ptr::null_mut();
            }
        }

        let original = Bytes::from(vec![1u8, 2, 3, 4, 5]);
        let mut ffi = FFI_Bytes::from(original.clone());
        ffi.release = foreign_release;

        let restored: Bytes = ffi.into();
        assert_eq!(restored, original);
        // Dropping the borrowed `Bytes` must run the foreign release.
        drop(restored);
    }

    #[test]
    fn dropping_without_conversion_releases() {
        let ffi = FFI_Bytes::from(Bytes::from(vec![0u8; 64]));
        drop(ffi);
    }
}
