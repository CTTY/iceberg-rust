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

//! Crate version query.

use std::ffi::c_void;

use crate::strings::{AllocateStringFn, StringSlice};

/// Version of this crate as recorded in `Cargo.toml`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the crate version (`CARGO_PKG_VERSION`) as an engine-owned string.
///
/// The string is produced by calling `alloc` once with the version bytes; the
/// callback's return value is returned unchanged. Returns null if the body
/// panics.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_ffi_version(alloc: AllocateStringFn) -> *mut c_void {
    ffi_guard!(|_msg| std::ptr::null_mut(), {
        alloc(StringSlice::borrowed(VERSION))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strings::test_support::{allocate_string, take};

    #[test]
    fn version_is_delivered_through_the_allocator() {
        let ptr = iceberg_ffi_version(allocate_string as AllocateStringFn);
        let s = take(ptr);
        assert_eq!(s.as_deref(), Some(VERSION));
        assert!(!VERSION.is_empty());
    }
}
