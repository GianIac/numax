#[link(wasm_import_module = "nx")]
unsafe extern "C" {
    // DB (namespace "nx")
    pub fn db_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32;
    pub fn db_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32;
    pub fn db_exists(key_ptr: u32, key_len: u32) -> i32;
    pub fn db_scan(
        prefix_ptr: u32,
        prefix_len: u32,
        cursor: u64,
        limit: u32,
        out_ptr: u32,
        out_cap: u32,
    ) -> i32;
    pub fn db_keys(
        prefix_ptr: u32,
        prefix_len: u32,
        cursor: u64,
        limit: u32,
        out_ptr: u32,
        out_cap: u32,
    ) -> i32;
    pub fn db_delete(key_ptr: u32, key_len: u32) -> i32;

    // CRDT (namespace "nx")
    pub fn crdt_gcounter_inc(key_ptr: u32, key_len: u32, delta: u64) -> i32;
    pub fn crdt_gcounter_value(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32;

    // Legacy: for compatibility with older guests / examples. Signature must remain (u32,u32)->().
    #[expect(dead_code, reason = "legacy guest import kept for compatibility")]
    pub fn host_log(msg_ptr: u32, msg_len: u32);

    // Preferred: allows error codes.
    pub fn host_log_v2(msg_ptr: u32, msg_len: u32) -> i32;
}
