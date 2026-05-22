use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("kv_sdk_roundtrip: start");

    db::set("hello", b"world").unwrap();
    nx_log!("kv_sdk_roundtrip: exists = {}", db::exists("hello").unwrap());

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
