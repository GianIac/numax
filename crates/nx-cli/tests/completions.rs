use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

fn run_nx(args: &[&str], working_directory: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_nx"))
        .args(args)
        .current_dir(working_directory)
        .output()
        .unwrap()
}

fn empty_working_directory() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    loop {
        let sequence = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "nx-completions-{}-{timestamp}-{sequence}",
            std::process::id()
        ));

        match fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                panic!(
                    "failed to create temporary directory {}: {error}",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn supported_shells_write_scripts_without_creating_files() {
    let working_directory = empty_working_directory();

    for shell in ["bash", "zsh", "fish", "powershell", "elvish"] {
        let output = run_nx(&["completions", shell], &working_directory);

        assert!(
            output.status.success(),
            "{shell}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!output.stdout.is_empty(), "empty stdout for {shell}");
        assert!(output.stderr.is_empty(), "unexpected stderr for {shell}");
        assert_eq!(fs::read_dir(&working_directory).unwrap().count(), 0);
    }

    fs::remove_dir(working_directory).unwrap();
}

#[test]
fn missing_shell_lists_the_required_argument() {
    let working_directory = empty_working_directory();
    let output = run_nx(&["completions"], &working_directory);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("required"), "{stderr}");
    assert!(stderr.contains("<SHELL>"), "{stderr}");

    fs::remove_dir(working_directory).unwrap();
}

#[test]
fn invalid_shell_lists_supported_values() {
    let working_directory = empty_working_directory();
    let output = run_nx(&["completions", "nushell"], &working_directory);

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    for shell in ["bash", "elvish", "fish", "powershell", "zsh"] {
        assert!(stderr.contains(shell), "missing {shell} in: {stderr}");
    }

    fs::remove_dir(working_directory).unwrap();
}
