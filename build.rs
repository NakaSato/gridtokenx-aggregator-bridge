fn main() -> Result<(), Box<dyn std::error::Error>> {
    let oracle_protos = &["proto/oracle.proto"];
    let oracle_includes = &["proto"];

    let identity_protos = &["../gridtokenx-iam-service/crates/iam-protocol/proto/identity.proto"];
    let identity_includes = &["../gridtokenx-iam-service/crates/iam-protocol/proto"];

    // 1. Generate ConnectRPC for Identity (Required for Authorization)
    connectrpc_build::Config::new()
        .files(identity_protos)
        .includes(identity_includes)
        .include_file("_identity_include.rs")
        .compile()?;

    // 2. Generate ConnectRPC for Oracle
    connectrpc_build::Config::new()
        .files(oracle_protos)
        .includes(oracle_includes)
        .include_file("_oracle_include.rs")
        .compile()?;

    Ok(())
}
