//! String transport across the C boundary.

use std::ffi::c_char;
use std::ffi::c_void;
use std::str::Utf8Error;

/// A borrowed, not NUL-terminated, UTF-8 byte range.
///
/// Used for every string passed into the crate and for every string handed to
/// an engine callback. The memory belongs to the caller and is only valid for
/// the duration of the call that receives it. `ptr` may be null when `len` is
/// zero.
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

    /// Views the slice as `&str`.
    ///
    /// # Safety
    ///
    /// `ptr` must point to `len` readable bytes that stay valid and unmodified
    /// for the returned lifetime, or `len` must be zero.
    pub unsafe fn as_str<'a>(&self) -> Result<&'a str, Utf8Error> {
        if self.len == 0 {
            return Ok("");
        }
        // SAFETY: the caller guarantees `ptr` is valid for `len` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(self.ptr.cast::<u8>(), self.len) };
        std::str::from_utf8(bytes)
    }
}

/// Engine callback that copies a [`StringSlice`] into an engine-owned string.
///
/// The returned pointer is opaque to Rust and is passed straight back to the
/// caller of the `extern "C"` function, so the engine chooses the owning type
/// (for DuckDB, a `string_t` in the result vector). The slice is only valid
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
}
