//! Fingerprints the engine's sources and migrations into
//! `TRELLIS_SOURCE_FINGERPRINT`, which keys the migrated template data
//! directory `testkit`'s clusters start from (see the `cluster` module doc).
//! Any change to the engine yields a new key, so a template built from older
//! code is never reused; the migrations alone would not catch a change to
//! what `migrate` does besides running them.

use std::fs;
use std::hash::{DefaultHasher, Hasher};
use std::path::{Path, PathBuf};

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../trellis");
    let mut files = Vec::new();
    for dir in ["src", "migrations"] {
        println!("cargo:rerun-if-changed={}", root.join(dir).display());
        collect(&root.join(dir), &mut files);
    }
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.toml").display()
    );
    files.push(root.join("Cargo.toml"));
    let lock = root.join("../Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock.display());
    files.push(lock);
    files.sort();
    // `DefaultHasher::new()` is unkeyed: the same input gives the same value on
    // every run of one toolchain, which is all a cache key needs.
    let mut hash = DefaultHasher::new();
    for file in &files {
        hash.write(
            file.strip_prefix(&root)
                .unwrap_or(file)
                .to_string_lossy()
                .as_bytes(),
        );
        hash.write(&[0]);
        hash.write(&fs::read(file).unwrap_or_default());
        hash.write(&[0]);
    }
    println!(
        "cargo:rustc-env=TRELLIS_SOURCE_FINGERPRINT={:016x}",
        hash.finish()
    );
}
