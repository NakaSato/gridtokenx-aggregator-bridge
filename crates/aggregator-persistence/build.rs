fn main() {
    // src/infra/db.rs embeds ../../migrations into MIGRATOR at compile time via
    // sqlx::migrate!; proc macros can't register file dependencies, so without
    // this a newly added migration file doesn't trigger a rebuild and the
    // migrate job silently runs with a stale embedded set.
    println!("cargo:rerun-if-changed=../../migrations");
}
