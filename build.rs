fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = &["../gridtokenx-iam-service/proto/identity.proto"];
    let includes = &["../gridtokenx-iam-service/proto"];

    connectrpc_build::Config::new()
        .files(protos)
        .includes(includes)
        .include_file("_identity_include.rs")
        .compile()?;

    Ok(())
}
