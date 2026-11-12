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

//! Toolchain proof calls: one through the tokio runtime, one through
//! iceberg-rust's spec types, and a test-only one that panics.
//!
//! These exist so the build pipeline, the result/error conventions and the
//! static link against iceberg-rust are exercised end to end before any table
//! functionality lands.

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

use crate::error::{FfiError, IcebergResultI32, IcebergResultU64};
use crate::handle::Shared;
use crate::runtime::Runtime;

/// Value returned by [`iceberg_runtime_smoke`].
pub const ICEBERG_SMOKE_ANSWER: u64 = 42;

/// Spawns a task on the runtime and returns its result ([`ICEBERG_SMOKE_ANSWER`]).
///
/// Proves that `block_on` works from an engine thread and that errors flow
/// back through the runtime's allocator.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_runtime_smoke(runtime: Shared<Runtime>) -> IcebergResultU64 {
    with_runtime!(runtime, IcebergResultU64, |rt| {
        let joined = rt.block_on(async {
            tokio::spawn(async {
                // Emitted on a worker thread so the log forwarding path
                // is exercised end to end.
                tracing::debug!(answer = ICEBERG_SMOKE_ANSWER, "runtime smoke task finished");
                ICEBERG_SMOKE_ANSWER
            })
            .await
        })?;
        joined.map_err(|e| {
            FfiError::new(
                crate::error::IcebergErrorCode::Unexpected,
                format!("smoke task failed: {e}"),
            )
        })
    })
}

/// Builds a three-column schema with iceberg-rust and returns its highest
/// field id (3).
///
/// Proves that the static library really links iceberg-rust code and that
/// `iceberg::Error` maps onto the FFI error codes.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_spec_smoke(runtime: Shared<Runtime>) -> IcebergResultI32 {
    with_runtime!(runtime, IcebergResultI32, |_rt| {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields([
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
            ])
            .build()?;
        Ok(schema.highest_field_id())
    })
}

/// Test-only entry point that panics, to verify the boundary guard.
#[cfg(test)]
pub extern "C" fn iceberg_test_panic(runtime: Shared<Runtime>) -> IcebergResultU64 {
    #[allow(clippy::panic)]
    fn boom() -> u64 {
        panic!("deliberate test panic")
    }
    with_runtime!(runtime, IcebergResultU64, |_rt| { Ok(boom()) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::test_support::take;
    use crate::error::{IcebergErrorCode, IcebergResultTag};
    use crate::runtime::iceberg_runtime_free;
    use crate::runtime::test_support::new_test_runtime;

    #[test]
    fn runtime_smoke_returns_the_answer_and_logs() {
        let rt = new_test_runtime();
        let r = iceberg_runtime_smoke(rt);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert_eq!(r.ok, ICEBERG_SMOKE_ANSWER);
        assert!(r.err.is_null());
        iceberg_runtime_free(rt);
        let captured = crate::logging::test_support::CAPTURED
            .lock()
            .map(|v| v.clone())
            .unwrap_or_default();
        assert!(captured.iter().any(|(c, level, target, m)| {
            *c == crate::logging::test_support::TEST_CTX
                && *level == crate::logging::IcebergLogLevel::Debug
                && target == "iceberg_ffi::smoke"
                && m == "runtime smoke task finished answer=42"
        }));
    }

    #[test]
    fn spec_smoke_touches_iceberg_rust() {
        let rt = new_test_runtime();
        let r = iceberg_spec_smoke(rt);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert_eq!(r.ok, 3);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn null_runtime_is_an_error_without_allocator() {
        let r = iceberg_runtime_smoke(Shared::null());
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert!(r.err.is_null());
        assert_eq!(r.ok, 0);
    }

    #[test]
    fn panic_is_converted_to_an_error() {
        let rt = new_test_runtime();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let r = iceberg_test_panic(rt);
        std::panic::set_hook(previous_hook);
        assert_eq!(r.tag, IcebergResultTag::Err);
        let e = take(r.err);
        assert_eq!(e.as_ref().map(|e| e.code), Some(IcebergErrorCode::Panic));
        assert!(
            e.as_ref()
                .map(|e| e.message.contains("deliberate test panic"))
                .unwrap_or(false)
        );
        iceberg_runtime_free(rt);
    }
}
