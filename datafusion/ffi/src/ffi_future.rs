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

//! FFI-safe async primitives, replacing the `async-ffi` crate.

use std::ffi::c_void;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// FFI-safe version of [`Poll`].
#[repr(C)]
pub enum FfiPoll<T> {
    Ready(T),
    Pending,
    Panicked,
}

/// FFI-safe wrapper around [`Context`] for passing wakers across FFI boundaries.
#[repr(C)]
pub struct FfiContext {
    data: *mut c_void,
}

impl FfiContext {
    /// Borrow back the original [`Context`].
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer is valid and derived from a real `Context`.
    pub unsafe fn with_context<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Context<'_>) -> R,
    {
        let cx = unsafe { &mut *(self.data as *mut Context<'_>) };
        f(cx)
    }
}

/// Extension trait on [`Context`] to produce an [`FfiContext`].
pub trait ContextExt {
    fn with_ffi_context<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut FfiContext) -> R;
}

impl ContextExt for Context<'_> {
    fn with_ffi_context<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut FfiContext) -> R,
    {
        let mut ffi_cx = FfiContext {
            data: self as *mut Context<'_> as *mut c_void,
        };
        f(&mut ffi_cx)
    }
}

/// An FFI-safe future. Wraps a boxed, pinned, `Send + 'static` future behind
/// `extern "C"` function pointers so it can cross shared-library boundaries.
#[repr(C)]
pub struct FfiFuture<T> {
    poll_fn: unsafe extern "C" fn(data: *mut c_void, context: *mut c_void) -> FfiPoll<T>,
    drop_fn: unsafe extern "C" fn(data: *mut c_void),
    data: *mut c_void,
}

unsafe impl<T: Send> Send for FfiFuture<T> {}

impl<T> FfiFuture<T> {
    fn new<F: Future<Output = T> + Send + 'static>(future: F) -> Self {
        let boxed: Box<Pin<Box<dyn Future<Output = T> + Send>>> =
            Box::new(Box::pin(future));
        let data = Box::into_raw(boxed) as *mut c_void;
        FfiFuture {
            poll_fn: poll_wrapper::<T>,
            drop_fn: drop_wrapper::<T>,
            data,
        }
    }
}

unsafe extern "C" fn poll_wrapper<T>(
    data: *mut c_void,
    context: *mut c_void,
) -> FfiPoll<T> {
    let future = unsafe { &mut *(data as *mut Pin<Box<dyn Future<Output = T> + Send>>) };
    let cx = unsafe { &mut *(context as *mut Context<'_>) };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        future.as_mut().poll(cx)
    })) {
        Ok(Poll::Ready(v)) => FfiPoll::Ready(v),
        Ok(Poll::Pending) => FfiPoll::Pending,
        Err(_) => FfiPoll::Panicked,
    }
}

unsafe extern "C" fn drop_wrapper<T>(data: *mut c_void) {
    drop(unsafe { Box::from_raw(data as *mut Pin<Box<dyn Future<Output = T> + Send>>) });
}

impl<T> Future for FfiFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        let result =
            unsafe { (this.poll_fn)(this.data, cx as *mut Context<'_> as *mut c_void) };
        match result {
            FfiPoll::Ready(v) => Poll::Ready(v),
            FfiPoll::Pending => Poll::Pending,
            FfiPoll::Panicked => {
                panic!("Panic occurred during poll on FfiFuture")
            }
        }
    }
}

impl<T> Drop for FfiFuture<T> {
    fn drop(&mut self) {
        unsafe { (self.drop_fn)(self.data) }
    }
}

/// Extension trait to convert any `Future + Send + 'static` into an [`FfiFuture`].
pub trait FutureExt: Future + Send + 'static + Sized {
    fn into_ffi(self) -> FfiFuture<Self::Output> {
        FfiFuture::new(self)
    }
}

impl<F: Future + Send + 'static> FutureExt for F {}
