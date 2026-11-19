//! Opaque handle types.
//!
//! Both handle kinds are pointer-sized, `#[repr(transparent)]` wrappers, so in
//! the C header they appear as pointers to opaque structs. Null is a valid
//! (invalid-handle) value for either; every `extern "C"` function checks for it
//! and reports [`IcebergErrorCode::InvalidHandle`](crate::error::IcebergErrorCode::InvalidHandle).

use std::sync::Arc;

/// Reference-counted handle, backed by an `Arc<T>`.
///
/// Safe to pass to any thread. The engine owns one strong reference per
/// handle value it received and releases it with the matching
/// `iceberg_*_free` call; Rust may hold further references internally.
#[repr(transparent)]
pub struct Shared<T> {
    ptr: *const T,
}

impl<T> Shared<T> {
    /// The null handle.
    pub const fn null() -> Self {
        Self {
            ptr: std::ptr::null(),
        }
    }

    /// Converts an `Arc` into a handle, transferring one strong count to the
    /// engine.
    pub fn from_arc(arc: Arc<T>) -> Self {
        Self {
            ptr: Arc::into_raw(arc),
        }
    }

    /// Whether the handle is null.
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    /// Borrows the value without touching the reference count.
    ///
    /// Returns `None` for a null handle.
    ///
    /// # Safety
    ///
    /// The handle must have come from [`Shared::from_arc`] and must not have
    /// been passed to [`Shared::into_arc`] since.
    pub unsafe fn borrow(&self) -> Option<&T> {
        if self.ptr.is_null() {
            None
        } else {
            // SAFETY: a non-null handle holds a strong count, so the
            // allocation is alive for as long as the handle is.
            Some(unsafe { &*self.ptr })
        }
    }

    /// Takes a new strong reference to the value behind the handle.
    ///
    /// The engine's own reference is untouched; the returned `Arc` is an
    /// additional one that Rust may keep for as long as it likes. Returns
    /// `None` for a null handle.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Shared::borrow`].
    pub unsafe fn clone_arc(&self) -> Option<Arc<T>> {
        if self.ptr.is_null() {
            None
        } else {
            // SAFETY: the pointer came from `Arc::into_raw` and the handle
            // holds a strong count, so incrementing and re-materialising a
            // second `Arc` is sound.
            unsafe {
                Arc::increment_strong_count(self.ptr);
                Some(Arc::from_raw(self.ptr))
            }
        }
    }

    /// Takes the engine's strong reference back as an `Arc`.
    ///
    /// Returns `None` for a null handle. After this call the handle value
    /// must not be used again.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Shared::borrow`]; additionally the caller must
    /// be giving up the engine's reference (this is the `free` path).
    pub unsafe fn into_arc(self) -> Option<Arc<T>> {
        if self.ptr.is_null() {
            None
        } else {
            // SAFETY: the pointer came from `Arc::into_raw`.
            Some(unsafe { Arc::from_raw(self.ptr) })
        }
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Handles are plain pointer values. Copying one does not touch the reference
/// count; the engine still owns exactly one reference per `from_arc`.
impl<T> Copy for Shared<T> {}

impl<T> std::fmt::Debug for Shared<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Shared({:p})", self.ptr)
    }
}

/// Single-owner handle, backed by a `Box<T>`.
///
/// The engine owns the value until it passes the handle to a consuming
/// function (documented per function) or to the matching `iceberg_*_free`.
#[repr(transparent)]
pub struct Exclusive<T> {
    ptr: *mut T,
}

impl<T> Clone for Exclusive<T> {
    fn clone(&self) -> Self {
        *self
    }
}

/// Handles are plain pointer values. The engine must not use a copy after the
/// original was consumed or freed.
impl<T> Copy for Exclusive<T> {}

impl<T> std::fmt::Debug for Exclusive<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Exclusive({:p})", self.ptr)
    }
}

impl<T> Exclusive<T> {
    /// The null handle.
    pub const fn null() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
        }
    }

    /// Moves a value onto the heap and hands ownership to the engine.
    pub fn from_box(value: Box<T>) -> Self {
        Self {
            ptr: Box::into_raw(value),
        }
    }

    /// Whether the handle is null.
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    /// Borrows the value mutably.
    ///
    /// Returns `None` for a null handle.
    ///
    /// # Safety
    ///
    /// The handle must have come from [`Exclusive::from_box`], must not have
    /// been passed to [`Exclusive::into_box`] since, and no other borrow of it
    /// may be live.
    pub unsafe fn borrow_mut(&mut self) -> Option<&mut T> {
        if self.ptr.is_null() {
            None
        } else {
            // SAFETY: a non-null handle uniquely owns the allocation.
            Some(unsafe { &mut *self.ptr })
        }
    }

    /// Takes ownership back as a `Box`.
    ///
    /// Returns `None` for a null handle. After this call the handle value
    /// must not be used again.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Exclusive::borrow_mut`].
    pub unsafe fn into_box(self) -> Option<Box<T>> {
        if self.ptr.is_null() {
            None
        } else {
            // SAFETY: the pointer came from `Box::into_raw`.
            Some(unsafe { Box::from_raw(self.ptr) })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_round_trip_keeps_count() {
        let arc = Arc::new(7u32);
        let probe = Arc::clone(&arc);
        let handle = Shared::from_arc(arc);
        assert_eq!(Arc::strong_count(&probe), 2);
        assert_eq!(unsafe { handle.borrow() }, Some(&7));
        assert_eq!(Arc::strong_count(&probe), 2);
        let back = unsafe { handle.into_arc() };
        assert_eq!(back.as_deref(), Some(&7));
        drop(back);
        assert_eq!(Arc::strong_count(&probe), 1);
    }

    #[test]
    fn clone_arc_adds_a_strong_count() {
        let arc = Arc::new(7u32);
        let probe = Arc::clone(&arc);
        let handle = Shared::from_arc(arc);
        let extra = unsafe { handle.clone_arc() };
        assert_eq!(extra.as_deref(), Some(&7));
        assert_eq!(Arc::strong_count(&probe), 3);
        drop(extra);
        assert_eq!(Arc::strong_count(&probe), 2);
        drop(unsafe { handle.into_arc() });
        assert_eq!(Arc::strong_count(&probe), 1);
    }

    #[test]
    fn null_handles_are_none() {
        let shared = Shared::<u32>::null();
        let mut exclusive = Exclusive::<u32>::null();
        assert!(unsafe { shared.borrow() }.is_none());
        assert!(unsafe { shared.clone_arc() }.is_none());
        assert!(unsafe { shared.into_arc() }.is_none());
        assert!(unsafe { exclusive.borrow_mut() }.is_none());
        assert!(unsafe { exclusive.into_box() }.is_none());
    }

    #[test]
    fn exclusive_round_trip() {
        let mut handle = Exclusive::from_box(Box::new(String::from("a")));
        unsafe { handle.borrow_mut() }
            .map(|s| s.push('b'))
            .unwrap_or_default();
        let back = unsafe { handle.into_box() };
        assert_eq!(back.as_deref(), Some(&"ab".to_owned()));
    }

    #[test]
    fn handles_are_pointer_sized() {
        assert_eq!(
            std::mem::size_of::<Shared<u8>>(),
            std::mem::size_of::<*const u8>()
        );
        assert_eq!(
            std::mem::size_of::<Exclusive<u8>>(),
            std::mem::size_of::<*mut u8>()
        );
    }
}
