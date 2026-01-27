#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "nx")]
unsafe extern "C" {
    pub fn host_log(ptr: i32, len: i32);

    pub fn db_set(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32) -> i32;
    pub fn db_get(key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32) -> i32;
    pub fn db_delete(key_ptr: i32, key_len: i32) -> i32;
}

// Stubs per build non-wasm
#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn host_log(_ptr: i32, _len: i32) {}

#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn db_set(_key_ptr: i32, _key_len: i32, _val_ptr: i32, _val_len: i32) -> i32 { -3 }

#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn db_get(_key_ptr: i32, _key_len: i32, _out_ptr: i32, _out_cap: i32) -> i32 { -3 }

#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn db_delete(_key_ptr: i32, _key_len: i32) -> i32 { -3 }
