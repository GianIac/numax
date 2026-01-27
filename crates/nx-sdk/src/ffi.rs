#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "nx")]
unsafe extern "C" {
    pub fn nx_host_log(ptr: i32, len: i32);

    pub fn nx_host_db_set(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32) -> i32;
    pub fn nx_host_db_get(key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32) -> i32;
    pub fn nx_host_db_delete(key_ptr: i32, key_len: i32) -> i32;
}


// Stubs per build non-wasm
#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn nx_host_log(_ptr: i32, _len: i32) {}

#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn nx_host_db_set(_k: i32, _kl: i32, _v: i32, _vl: i32) -> i32 { -3 }

#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn nx_host_db_get(_k: i32, _kl: i32, _o: i32, _oc: i32) -> i32 { -3 }

#[cfg(not(target_arch = "wasm32"))]
pub unsafe fn nx_host_db_delete(_k: i32, _kl: i32) -> i32 { -3 }
