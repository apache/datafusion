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

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

#[tokio::main]
async fn main() -> Result<(), String> {
    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");
    println!("cargo:rerun-if-changed=proto/datafusion.proto");

    build().await?;

    Ok(())
}

async fn build() -> Result<(), String> {
    use std::io::Write;

    let out = std::path::PathBuf::from(
        std::env::var("OUT_DIR").expect("Cannot find OUT_DIR environment variable"),
    );
    let descriptor_path = out.join("proto_descriptor.bin");

    // compute protoc distribution URL
    let host = std::env::var("HOST").expect("HOST not specified!");
    let (proto_platform, suffix) = match host.as_str() {
        "todo" => ("linux-aarch_64", ""), // TODO: arm
        "x86_64-unknown-linux-gnu" => ("linux-x86_64", ""),
        "x86_64-pc-windows-msvc" => ("win64", ".exe"),
        "x86_64-apple-darwin" => ("osx-x86_64", ""),
        _ => panic!("No protobuf found for OS type: {}", host),
    };
    let proto_base = "https://github.com/protocolbuffers/protobuf/releases/download";
    let proto_ver = "21.8";
    let proto_url =
        format!("{proto_base}/v{proto_ver}/protoc-{proto_ver}-{proto_platform}.zip");

    // Download protoc binary
    let target_dir = out.join("protoc");
    if !target_dir.exists() {
        let archive = reqwest::get(proto_url)
            .await
            .expect("Can't download protoc");
        let archive = archive.bytes().await.expect("Can't download protoc");
        let archive = std::io::Cursor::new(archive);
        zip_extract::extract(archive, &target_dir, true).expect("Can't extract protoc");
    }
    std::env::set_var("PROTOC", out.join(format!("protoc/bin/protoc{suffix}")));

    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&["proto/datafusion.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {}", e))?;

    #[cfg(feature = "json")]
    let descriptor_set = std::fs::read(&descriptor_path)
        .expect(&*format!("Cannot read {:?}", &descriptor_path));

    #[cfg(feature = "json")]
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)
        .expect(&*format!(
            "Cannot register descriptors {:?}",
            &descriptor_set
        ))
        .build(&[".datafusion"])
        .map_err(|e| format!("pbjson compilation failed: {}", e))?;

    // .serde.rs is not a valid package name, so append to datafusion.rs so we can treat it normally
    let proto = std::fs::read_to_string(out.join("datafusion.rs")).unwrap();

    #[cfg(feature = "json")]
    let json = std::fs::read_to_string(out.join("datafusion.serde.rs")).unwrap();

    #[cfg(feature = "docsrs")]
    let path = out.join("datafusion.rs");
    #[cfg(not(feature = "docsrs"))]
    let path = "src/generated/datafusion.rs";

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        .unwrap();
    file.write_all(proto.as_str().as_ref()).unwrap();

    #[cfg(feature = "json")]
    file.write_all(json.as_str().as_ref()).unwrap();

    Ok(())
}
