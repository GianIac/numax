unsafe extern "C" {
    // DB (namespace "nx")
    pub fn db_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32;
    pub fn db_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32;
    pub fn db_delete(key_ptr: u32, key_len: u32) -> i32;

    // LOG (namespace "nx")
    pub fn host_log(msg_ptr: u32, msg_len: u32) -> i32;
}
