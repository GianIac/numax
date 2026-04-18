#[link(wasm_import_module = "nx")]
extern "C" {
    fn host_log(ptr: i32, len: i32);

    fn db_set(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32) -> i32;
    fn db_get(key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32) -> i32;
    fn db_delete(key_ptr: i32, key_len: i32) -> i32;
}

fn log_str(s: &str) {
    unsafe { host_log(s.as_ptr() as i32, s.len() as i32) }
}

#[no_mangle]
pub extern "C" fn run() {
    log_str("kv_roundtrip: start");

    let key = b"hello";
    let val = b"world";

    // 1) db_set
    let rc = unsafe {
        db_set(
            key.as_ptr() as i32,
            key.len() as i32,
            val.as_ptr() as i32,
            val.len() as i32,
        )
    };

    if rc != 0 {
        log_str("kv_roundtrip: db_set failed");
        return;
    }
    log_str("kv_roundtrip: db_set ok");

    // 2) db_get (Gotcha: pre-allocated buffer)
    let mut out_buf = vec![0u8; 64];

    let n = unsafe {
        db_get(
            key.as_ptr() as i32,
            key.len() as i32,
            out_buf.as_mut_ptr() as i32,
            out_buf.len() as i32,
        )
    };

    // Gotcha: handle error codes, especially -2
    if n == -1 {
        log_str("kv_roundtrip: db_get -> not found (-1)");
        return;
    }
    if n == -2 {
        log_str("kv_roundtrip: db_get -> buffer too small (-2)");
        return;
    }
    if n == -3 {
        log_str("kv_roundtrip: db_get -> internal error (-3)");
        return;
    }
    if n < 0 {
        log_str("kv_roundtrip: db_get -> unknown negative error");
        return;
    }

    let n = n as usize;
    if n > out_buf.len() {
        log_str("kv_roundtrip: db_get -> returned length > buffer (unexpected)");
        return;
    }

    let bytes = &out_buf[..n];
    match std::str::from_utf8(bytes) {
        Ok(s) => {
            log_str("kv_roundtrip: db_get ok, value=");
            log_str(s);
        }
        Err(_) => {
            log_str("kv_roundtrip: db_get ok, value is not utf8");
        }
    }

    // 3) db_delete("hello")
    let del_rc = unsafe { db_delete(key.as_ptr() as i32, key.len() as i32) };
    if del_rc != 0 {
        log_str("kv_roundtrip: db_delete failed");
        return;
    }
    log_str("kv_roundtrip: db_delete ok");

    // 4) db_get after delete -> should return -1 (not found)
    let mut out_buf2 = vec![0u8; 64];
    let n2 = unsafe {
        db_get(
            key.as_ptr() as i32,
            key.len() as i32,
            out_buf2.as_mut_ptr() as i32,
            out_buf2.len() as i32,
        )
    };

    if n2 == -1 {
        log_str("kv_roundtrip: db_get after delete -> not found (ok)");
    } else if n2 == -2 {
        log_str("kv_roundtrip: db_get after delete -> buffer too small (-2) (unexpected)");
    } else if n2 == -3 {
        log_str("kv_roundtrip: db_get after delete -> internal error (-3)");
    } else if n2 >= 0 {
        log_str("kv_roundtrip: db_get after delete -> unexpected value (should be not found)");
    } else {
        log_str("kv_roundtrip: db_get after delete -> unknown negative error");
    }

    log_str("kv_roundtrip: done");
}