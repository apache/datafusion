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

use std::path::Path;

fn main() -> Result<(), String> {
    let proto_dir = Path::new("proto");
    let common_proto = Path::new("proto/datafusion_common.proto");
    let datafusion_proto = Path::new("proto/datafusion.proto");

    let descriptor_path = proto_dir.join("proto_descriptor.bin");

    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .out_dir("src")
        .compile_well_known_types()
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&[common_proto, datafusion_proto], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let descriptor_set = std::fs::read(&descriptor_path)
        .unwrap_or_else(|e| panic!("Cannot read {:?}: {}", &descriptor_path, e));

    pbjson_build::Builder::new()
        .out_dir("src")
        .register_descriptors(&descriptor_set)
        .unwrap_or_else(|e| {
            panic!("Cannot register descriptors {:?}: {}", &descriptor_set, e)
        })
        .build(&[".datafusion_common", ".datafusion"])
        .map_err(|e| format!("pbjson compilation failed: {e}"))?;

    // prost emits one file per package; pbjson emits one per package.
    // Copy each into src/generated/ under stable names that mod.rs `include!`s.
    for (src, dst) in [
        (
            "src/datafusion_common.rs",
            "src/generated/datafusion_common.rs",
        ),
        ("src/datafusion.rs", "src/generated/datafusion.rs"),
        (
            "src/datafusion_common.serde.rs",
            "src/generated/datafusion_common.serde.rs",
        ),
        (
            "src/datafusion.serde.rs",
            "src/generated/datafusion.serde.rs",
        ),
    ] {
        std::fs::copy(src, dst).unwrap();
    }

    Ok(())
}
