//! Fingerprints the engine's sources and migrations, and testkit's cluster
//! code, into `TRELLIS_SOURCE_FINGERPRINT`, which keys the migrated template data
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
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    // The engine, and how testkit builds the template (and the `initdb` it
    // runs), which is its own cluster code.
    for dir in ["trellis/src", "trellis/migrations", "testkit/src/cluster"] {
        collect(&workspace.join(dir), &mut files);
    }
    for file in ["trellis/Cargo.toml", "Cargo.lock", "testkit/src/cluster.rs"] {
        files.push(workspace.join(file));
    }
    for dir in ["trellis/src", "trellis/migrations", "testkit/src/cluster"] {
        println!("cargo:rerun-if-changed={}", workspace.join(dir).display());
    }
    for file in ["trellis/Cargo.toml", "Cargo.lock", "testkit/src/cluster.rs"] {
        println!("cargo:rerun-if-changed={}", workspace.join(file).display());
    }
    files.sort();
    // `DefaultHasher::new()` is unkeyed: the same input gives the same value on
    // every run of one toolchain, which is all a cache key needs. Paths are
    // hashed relative to the workspace, so two checkouts of the same code
    // share a template.
    let mut hash = DefaultHasher::new();
    for file in &files {
        hash.write(
            file.strip_prefix(&workspace)
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
