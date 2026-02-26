use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // recursively find all json files in the src directory
    glob::glob("src/**/*.json")
        .unwrap()
        .filter_map(Result::ok)
        .for_each(|path| {
            println!("cargo:rerun-if-changed={}", path.display());
        });

    // Build Prometheus protobuf definitions
    let proto_files = &[
        "proto/types.proto",
        "proto/remote.proto",
    ];

    // Configure the output directory
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_server(false)
        .build_client(false)
        .out_dir(&out_dir)
        .include_file("prometheus_proto.rs")
        .compile_protos(proto_files, &["proto/"])?;

    println!("cargo:rerun-if-changed=proto/");
    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}
