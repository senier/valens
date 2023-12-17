#![deny(clippy::pedantic)]

use std::process::Command;

fn main() {
    let output = Command::new("poetry")
        .args(["version", "-s"])
        .output()
        .unwrap();
    let version = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-env=VALENS_VERSION={version}");
}
