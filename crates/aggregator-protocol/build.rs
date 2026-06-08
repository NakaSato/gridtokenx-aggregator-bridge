fn main() -> Result<(), Box<dyn std::error::Error>> {
    let oracle_protos = &["proto/oracle.proto"];
    let oracle_includes = &["proto"];

    // identity.proto is owned by the IAM service; path is relative to this crate dir.
    let identity_protos =
        &["../../../gridtokenx-iam-service/crates/iam-protocol/proto/identity.proto"];
    let identity_includes = &["../../../gridtokenx-iam-service/crates/iam-protocol/proto"];

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

    // 3. Generate ConnectRPC for Dispatch
    connectrpc_build::Config::new()
        .files(&["proto/dispatch.proto"])
        .includes(&["proto"])
        .include_file("_dispatch_include.rs")
        .compile()?;

    Ok(())
}
