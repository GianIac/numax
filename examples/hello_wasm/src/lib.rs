// namespace: "nx", name: "host_log"
#[link(wasm_import_module = "nx")]
extern "C" {
    fn host_log(ptr: i32, len: i32);
}

fn log_str(s: &str) {
    let bytes = s.as_bytes();
    unsafe {
        host_log(bytes.as_ptr() as i32, bytes.len() as i32);
    }
}

#[no_mangle]
pub extern "C" fn run() {
    log_str("Hello from WASM by NumaX !!");
}
