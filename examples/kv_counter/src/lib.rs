use nx_sdk::{db, nx_log};

const KEY: &str = "counter";

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    // store comes from our own code: we store ASCII digits
    core::str::from_utf8(bytes).ok()?.parse::<u64>().ok()
}

#[no_mangle]
pub extern "C" fn run() {
    // 1) read counter (default 0)
    let current = match db::get(KEY) {
        Ok(Some(v)) => parse_u64(&v).unwrap_or(0),
        Ok(None) => 0,
        Err(_) => {
            nx_log!("kv_counter: db_get failed");
            return;
        }
    };

    // 2) increment
    let next = current.saturating_add(1);

    // 3) persist
    let s = nx_sdk::__alloc::format!("{}", next); // avoids std::format! assumptions
    if let Err(_) = db::set(KEY, s.as_bytes()) {
        nx_log!("kv_counter: db_set failed");
        return;
    }

    // 4) print updated value
    nx_log!("kv_counter: {}", next);
}
