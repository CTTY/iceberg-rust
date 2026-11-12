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

//! C ABI over [apache/iceberg-rust](https://github.com/apache/iceberg-rust) for
//! the `iceberg_rs` DuckDB extension.
//!
//! The crate is built as a static library and linked into the C++ extension.
//! Its public surface is the set of `extern "C"` functions and `#[repr(C)]`
//! types below; the C header is generated from them by cbindgen in
//! `build.rs`.
//!
//! # Conventions
//!
//! - **Handles** ([`handle`]): opaque pointers. [`Shared<T>`](handle::Shared)
//!   is reference counted and safe to use from any thread;
//!   [`Exclusive<T>`](handle::Exclusive) has a single owner. Each handle type
//!   has exactly one `iceberg_*_free` function.
//! - **Results** ([`error`]): fallible functions return a concrete
//!   `#[repr(C)]` result struct (`IcebergResultRuntime`, `IcebergResultU64`,
//!   ...) carrying a tag, the success payload, and an error pointer. The
//!   error object is allocated by the engine through the
//!   [`AllocateErrorFn`](error::AllocateErrorFn) it registered, so Rust never
//!   owns error memory.
//! - **Strings** ([`strings`]): borrowed in as
//!   [`StringSlice`](strings::StringSlice); returned through an engine-supplied
//!   [`AllocateStringFn`](strings::AllocateStringFn) so the engine picks the
//!   owning type.
//! - **Runtime** ([`runtime`]): the engine creates one tokio runtime per
//!   database and passes it to every call that needs async work. Rust never
//!   spawns a runtime on its own.
//! - **File IO** ([`storage`]): all reads go through an
//!   [`IcebergFileSystemVTable`](storage::IcebergFileSystemVTable) the engine
//!   supplies; the crate links no file system or network code of its own.
//! - **Tables** ([`table`], [`schema`]): `iceberg_table_load` reads a metadata
//!   file through the vtable and returns a `Shared<Table>`; accessors expose
//!   snapshots, properties, partition specs and the schema (through a
//!   callback-driven [`IcebergSchemaVisitor`](schema::IcebergSchemaVisitor)).
//!   Optional scalars travel as [`IcebergOptionalI64`](optional::IcebergOptionalI64)
//!   and friends.
//! - **Scan planning** ([`scan`]): an `Exclusive<ScanBuilder>` collects the
//!   snapshot, projection and predicate; `iceberg_scan_builder_build`
//!   consumes it, plans every file task eagerly and returns a
//!   `Shared<Scan>` whose tasks (`Shared<FileTask>`) expose the data file,
//!   projected field ids, identity partition constants and delete files.
//! - **Predicates** ([`predicate`]): the engine builds a filter by calling
//!   `iceberg_pred_*` constructors against an opaque arena
//!   ([`IcebergPredicateState`](predicate::IcebergPredicateState)) and
//!   returning the root node id; the first failure poisons the arena so a
//!   partial predicate is never planned.
//! - **Literals** ([`literal`]): primitive values cross the boundary as the
//!   tagged `#[repr(C)]` [`IcebergLiteral`](literal::IcebergLiteral), used for
//!   predicate constants and partition constants alike.
//! - **Panics**: every `extern "C"` function body runs inside the
//!   `ffi_guard!` macro; a panic is converted into an error result with code
//!   [`IcebergErrorCode::Panic`](error::IcebergErrorCode::Panic) instead of
//!   crossing the boundary.

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(rustdoc::broken_intra_doc_links)]

#[macro_use]
mod guard;

pub mod error;
pub mod handle;
pub mod literal;
pub mod logging;
pub mod optional;
pub mod predicate;
pub mod runtime;
pub mod scan;
pub mod schema;
pub mod storage;
pub mod strings;
pub mod table;

pub mod smoke;
pub mod version;
