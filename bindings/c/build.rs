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
//! The header is written next to the compiled static library, in the cargo
//! profile directory (`target/<triple>/<profile>/ffi-headers/iceberg_ffi.h`),
//! so the CMake side can locate it from the same variables it uses to locate
//! `libiceberg_ffi.a`. The profile directory is three levels above `OUT_DIR`
//! (`<profile>/build/<pkg>-<hash>/out`).

use std::env;
use std::path::PathBuf;

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
    let header = header_dir.join("iceberg_ffi.h");

    let config = cbindgen::Config::from_file(crate_dir.join("cbindgen.toml")).unwrap_or_else(|e| {
        panic!("cannot read cbindgen.toml: {e}");
    });
    let bindings = cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
        .unwrap_or_else(|e| panic!("cbindgen failed: {e}"));
    bindings.write_to_file(&header);
}
