//! Detects a built frontend bundle and wires it into the compilation.
//!
//! When `frontend/dist/index.html` exists relative to the repository root,
//! the script sets the `ct_ui_embedded` cfg and exports the absolute bundle
//! path through `CT_UI_DIST_DIR` so `ui.rs` can embed the directory with
//! `include_dir!`. Without a bundle the crate compiles a minimal placeholder
//! shell instead, so the read-only API stays fully functional.

use std::env;
use std::path::{Component, Path, PathBuf};

/// Location of the Vite build output relative to this crate's manifest.
const DIST_RELATIVE_TO_CRATE: &str = "../../../frontend/dist";

fn main() {
    println!("cargo::rustc-check-cfg=cfg(ct_ui_embedded)");
    let manifest_dir =
        env::var("CARGO_MANIFEST_DIR").expect("cargo always provides CARGO_MANIFEST_DIR");
    let dist_dir = normalize_lexically(&Path::new(&manifest_dir).join(DIST_RELATIVE_TO_CRATE));
    // Watching the bundle root makes cargo re-evaluate this script whenever
    // the bundle changes, appears, or disappears, so the embedded/placeholder
    // decision never requires a manual `cargo clean`.
    println!("cargo::rerun-if-changed={}", dist_dir.display());
    if dist_dir.join("index.html").is_file() {
        println!("cargo::rustc-cfg=ct_ui_embedded");
        println!("cargo::rustc-env=CT_UI_DIST_DIR={}", dist_dir.display());
    }
}

/// Removes `.` and resolves `..` components without touching the filesystem.
///
/// `std::fs::canonicalize` would also work but yields `\\?\`-prefixed paths
/// on Windows, which are noisy in build logs and unnecessary here because
/// `CARGO_MANIFEST_DIR` is already absolute.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}
