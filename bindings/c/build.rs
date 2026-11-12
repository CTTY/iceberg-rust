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

//! Generates the C header for the crate with cbindgen.
//!
//! The header is written to two places:
//!
//! - next to the compiled static library, in the cargo profile directory
//!   (`target/<triple>/<profile>/ffi-headers/iceberg_ffi.h`), so a build
//!   system that already knows where `libiceberg_ffi.a` is can find it from
//!   the same variables. The profile directory is three levels above
//!   `OUT_DIR` (`<profile>/build/<pkg>-<hash>/out`);
//! - `include/iceberg_ffi.h` in the crate, which is committed so the C API
//!   is reviewable in diffs and usable without a Rust toolchain. The build
//!   fails when that copy is out of date; set `ICEBERG_FFI_UPDATE_HEADER=1`
//!   (or run `make generate-c-header` at the repository root) to rewrite it.

use std::env;
use std::path::{Path, PathBuf};

const COMMITTED_HEADER: &str = "include/iceberg_ffi.h";

fn main() {
    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|e| {
        panic!("CARGO_MANIFEST_DIR is not set: {e}");
    }));
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap_or_else(|e| {
        panic!("OUT_DIR is not set: {e}");
    }));

    println!("cargo:rerun-if-changed=cbindgen.toml");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={COMMITTED_HEADER}");
    println!("cargo:rerun-if-env-changed=ICEBERG_FFI_UPDATE_HEADER");

    let profile_dir = out_dir.ancestors().nth(3).unwrap_or_else(|| {
        panic!(
            "OUT_DIR has fewer than three ancestors: {}",
            out_dir.display()
        )
    });
    let header_dir = profile_dir.join("ffi-headers");
    std::fs::create_dir_all(&header_dir).unwrap_or_else(|e| {
        panic!("cannot create {}: {e}", header_dir.display());
    });
    let generated = header_dir.join("iceberg_ffi.h");

    let config = cbindgen::Config::from_file(crate_dir.join("cbindgen.toml")).unwrap_or_else(|e| {
        panic!("cannot read cbindgen.toml: {e}");
    });
    let bindings = cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
        .unwrap_or_else(|e| panic!("cbindgen failed: {e}"));
    bindings.write_to_file(&generated);

    sync_committed_header(&generated, &crate_dir.join(COMMITTED_HEADER));
}

/// Keeps the committed header in step with the generated one.
fn sync_committed_header(generated: &Path, committed: &Path) {
    let fresh = std::fs::read(generated).unwrap_or_else(|e| {
        panic!("cannot read {}: {e}", generated.display());
    });
    let update = env::var_os("ICEBERG_FFI_UPDATE_HEADER").is_some_and(|v| v == "1");
    if update {
        if let Some(parent) = committed.parent() {
            std::fs::create_dir_all(parent).unwrap_or_else(|e| {
                panic!("cannot create {}: {e}", parent.display());
            });
        }
        std::fs::write(committed, &fresh).unwrap_or_else(|e| {
            panic!("cannot write {}: {e}", committed.display());
        });
        return;
    }
    let current = std::fs::read(committed).unwrap_or_else(|e| {
        panic!(
            "{} is missing ({e}); run `ICEBERG_FFI_UPDATE_HEADER=1 cargo build -p iceberg-ffi` \
             (or `make generate-c-header`) to create it",
            committed.display()
        );
    });
    if current != fresh {
        panic!(
            "{} is out of date with the Rust sources; run \
             `ICEBERG_FFI_UPDATE_HEADER=1 cargo build -p iceberg-ffi` (or `make generate-c-header`) \
             and commit the result",
            committed.display()
        );
    }
}
