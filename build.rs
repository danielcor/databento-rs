fn main() {
    // Notify Cargo to rerun the build script if certain files change
    println!("cargo:rerun-if-changed=src/ffi.rs");
    println!("cargo:rerun-if-changed=src/examples/es_futures_pmz.rs");

    // No more UniFFI code generation needed
}