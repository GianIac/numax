/*
 * import `host_log_v2` from the `nx` namespace.
 * strings are passed as: (pointer, length)
 * signature: (u32, u32) -> i32
 */
__attribute__((import_module("nx")))
__attribute__((import_name("host_log_v2")))
extern int host_log_v2(const char* ptr, int len);

/*
 * IMPORTANT: import `db_set` from the `nx` namespace.
 * writes a key/value pair into the embedded datastore.
 */
__attribute__((import_module("nx")))
__attribute__((import_name("db_set")))
extern int db_set(
    const char* key_ptr,
    int key_len,
    const char* val_ptr,
    int val_len
);

/*
 * exported guest entrypoint expected by Numax.
 */
__attribute__((export_name("run")))
void run() {
    const char msg[] = "Hello from C guest!";
    host_log_v2(msg, sizeof(msg) - 1);

    const char* key = "hello";
    const char* val = "numax";

    db_set(key, 5, val, 5);

    const char done[] = "db_set ok";
    host_log_v2(done, sizeof(done) - 1);
}