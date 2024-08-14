fn main() {
    println!("cargo:rerun-if-changed=../protos/connection_manager.proto ");
    tonic_build::configure()
    .out_dir("src/connection_manager")
    .include_file("mod.rs")
    //.type_attribute("ConnectRequest", "#[derive(serde::Deserialize, serde::Serialize)]")
    //.type_attribute("ConnectResponse", "#[derive(serde::Deserialize, serde::Serialize)]")
    .compile(
        &["protos/connection_manager.proto"],
        &["protos"]
    )
    .unwrap();
}