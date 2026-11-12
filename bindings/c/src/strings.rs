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

//! String transport across the C boundary.

use std::ffi::{c_char, c_void};
use std::str::Utf8Error;

/// A borrowed, not NUL-terminated byte range; UTF-8 unless documented
/// otherwise.
///
/// Used for every string passed into the crate and for every string handed to
/// an engine callback. The memory belongs to the caller and is only valid for
/// the duration of the call that receives it. `ptr` may be null when `len` is
/// zero. Functions that carry raw bytes (binary, fixed and uuid literals)
/// reuse this type and say so; such payloads need not be UTF-8.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StringSlice {
    /// Start of the bytes; may be null if `len == 0`.
    pub ptr: *const c_char,
    /// Number of bytes.
    pub len: usize,
}

impl StringSlice {
    /// An empty slice with a null pointer.
    pub const fn empty() -> Self {
        Self {
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    /// Borrows `s` for as long as `s` lives.
    pub fn borrowed(s: &str) -> Self {
        Self {
            ptr: s.as_ptr().cast::<c_char>(),
            len: s.len(),
        }
    }

    /// Borrows raw `bytes` for as long as they live.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr().cast::<c_char>(),
            len: bytes.len(),
        }
    }

    /// Views the slice as raw bytes.
    ///
    /// # Safety
    ///
    /// `ptr` must point to `len` readable bytes that stay valid and unmodified
    /// for the returned lifetime, or `len` must be zero.
    pub unsafe fn as_bytes<'a>(&self) -> &'a [u8] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: the caller guarantees `ptr` is valid for `len` bytes.
        unsafe { std::slice::from_raw_parts(self.ptr.cast::<u8>(), self.len) }
    }

    /// Views the slice as `&str`.
    ///
    /// # Safety
    ///
    /// See [`StringSlice::as_bytes`].
    pub unsafe fn as_str<'a>(&self) -> Result<&'a str, Utf8Error> {
        // SAFETY: forwarded to the caller.
        std::str::from_utf8(unsafe { self.as_bytes() })
    }
}

/// Engine callback that copies a [`StringSlice`] into an engine-owned string.
///
/// The returned pointer is opaque to Rust and is passed straight back to the
/// caller of the `extern "C"` function, so the engine chooses the owning type
/// (for example its own string type in a result vector). The slice is only valid
/// during the callback.
pub type AllocateStringFn = extern "C" fn(StringSlice) -> *mut c_void;

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// Test allocator: copies the slice into a boxed `String` and leaks it as
    /// the opaque pointer. Tests reclaim it with [`take`].
    pub(crate) extern "C" fn allocate_string(slice: StringSlice) -> *mut c_void {
        // SAFETY: the slice is valid for the duration of the callback.
        let s = unsafe { slice.as_str() }
            .unwrap_or("<invalid utf-8>")
            .to_owned();
        Box::into_raw(Box::new(s)).cast::<c_void>()
    }

    /// Reclaims a pointer produced by [`allocate_string`].
    pub(crate) fn take(ptr: *mut c_void) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` came from `Box::into_raw` in `allocate_string`.
        Some(*unsafe { Box::from_raw(ptr.cast::<String>()) })
    }

    #[test]
    fn round_trip() {
        let s = "héllo";
        let slice = StringSlice::borrowed(s);
        assert_eq!(unsafe { slice.as_str() }.ok(), Some(s));
        assert_eq!(unsafe { StringSlice::empty().as_str() }.ok(), Some(""));
    }

    #[test]
    fn from_bytes_round_trip() {
        let raw = [0u8, 255, 7];
        let slice = StringSlice::from_bytes(&raw);
        assert_eq!(unsafe { slice.as_bytes() }, &raw);
        assert!(unsafe { slice.as_str() }.is_err());
        let empty: &[u8] = &[];
        assert_eq!(unsafe { StringSlice::from_bytes(empty).as_bytes() }, empty);
    }
}
