use std::process::Command;

#[test]
fn run_with_a_missing_module_fails_with_an_actionable_message() {
    let output = Command::new(env!("CARGO_BIN_EXE_nx"))
        .args(["run", "does_not_exist.wasm"])
        .current_dir(std::env::temp_dir())
        .output()
        .unwrap();

    assert!(!output.status.success(), "expected a non-zero exit status");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("WASM module not found at `does_not_exist.wasm`"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("hint: did you build it?"),
        "stderr: {stderr}"
    );
}
