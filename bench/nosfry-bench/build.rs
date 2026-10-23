//! Embeds the harness git sha as `GIT_SHA` for the config echo in every
//! run's final JSON line. Tolerates a missing `.git` or `git` binary
//! (e.g. a copied source tree) by falling back to `unknown`.

use std::process::Command;

fn main() {
    let sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=GIT_SHA={sha}");
}
