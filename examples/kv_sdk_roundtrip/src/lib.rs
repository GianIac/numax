use nx_sdk::{crypto, db, nx_log, system, time};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("kv_sdk_roundtrip: start");

    db::set("hello", b"world").unwrap();
    nx_log!("kv_sdk_roundtrip: exists = {}", db::exists("hello").unwrap());
    nx_log!(
        "kv_sdk_roundtrip: scan count = {}",
        db::scan("hel").unwrap().len()
    );
    nx_log!(
        "kv_sdk_roundtrip: keys count = {}",
        db::keys("hel").unwrap().len()
    );
    nx_log!("kv_sdk_roundtrip: time_now = {}", time::now());
    nx_log!(
        "kv_sdk_roundtrip: time_monotonic = {}",
        time::monotonic()
    );
    nx_log!(
        "kv_sdk_roundtrip: random bytes = {}",
        crypto::random_bytes(8).unwrap().len()
    );
    nx_log!(
        "kv_sdk_roundtrip: sha256 bytes = {}",
        crypto::hash_sha256(b"hello").unwrap().len()
    );
    nx_log!(
        "kv_sdk_roundtrip: blake3 bytes = {}",
        crypto::hash_blake3(b"hello").unwrap().len()
    );
    nx_log!("kv_sdk_roundtrip: module_id = {}", system::module_id().unwrap());
    nx_log!(
        "kv_sdk_roundtrip: capabilities = {}",
        system::host_capabilities().unwrap().len()
    );
    system::event_emit("kv_sdk_roundtrip.completed_db_setup", b"hello").unwrap();
    nx_log!(
        "kv_sdk_roundtrip: env present = {}",
        system::env_get("NX_EXAMPLE").unwrap().is_some()
    );

    let v = db::get("hello").unwrap().unwrap();
    nx_log!("kv_sdk_roundtrip: got {}", core::str::from_utf8(&v).unwrap());

    db::delete("hello").unwrap();
    let after = db::get("hello").unwrap();
    nx_log!("kv_sdk_roundtrip: after delete = {}", after.is_none());
    nx_log!(
        "kv_sdk_roundtrip: exists after delete = {}",
        db::exists("hello").unwrap()
    );

    nx_log!("kv_sdk_roundtrip: done");
}
