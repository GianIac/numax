+use std::fs;
 use std::process::Command;
+use std::time::{SystemTime, UNIX_EPOCH};

 #[test]
 fn run_with_a_missing_module_fails_with_an_actionable_message() {
+    let unique = SystemTime::now()
+        .duration_since(UNIX_EPOCH)
+        .unwrap()
+        .as_nanos();
+    let working_directory = std::env::temp_dir().join(format!(
+        "nx-run-missing-module-{}-{unique}",
+        std::process::id()
+    ));
+    fs::create_dir(&working_directory).unwrap();
+
     let output = Command::new(env!("CARGO_BIN_EXE_nx"))
         .args(["run", "does_not_exist.wasm"])
-        .current_dir(std::env::temp_dir())
+        .current_dir(&working_directory)
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
+
+    fs::remove_dir_all(working_directory).unwrap();
 }
