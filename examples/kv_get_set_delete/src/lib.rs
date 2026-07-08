use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("kv_get_set_delete: start");

    nx_log!("Setting key 'user:1'");
    match db::set("user:1", b"alice") {
        Ok(_) => nx_log!("Set successful"),
        Err(e) => {
            nx_log!("Set failed: {:?}", e);
            return;
        }
    }

    nx_log!("Getting key 'user:1'");
    match db::get("user:1") {
        Ok(Some(value)) => {
            let value = String::from_utf8_lossy(&value);
            nx_log!("Value: {}", value);
        }
        Ok(None) => {
            nx_log!("Key not found");
            return;
        }
        Err(e) => {
            nx_log!("Get failed: {:?}", e);
            return;
        }
    }

    nx_log!("Checking for 'user:1' existence");
    match db::exists("user:1") {
        Ok(exists) => nx_log!("Exists: {}", exists),
        Err(e) => {
            nx_log!("Exists failed: {:?}", e);
            return;
        }
    }

    nx_log!("Deleting Key 'user:1'");
    match db::delete("user:1") {
        Ok(_) => nx_log!("Delete successful"),
        Err(e) => {
            nx_log!("Delete failed: {:?}", e);
            return;
        }
    }

    nx_log!("Checking for 'user:1' existence");
    match db::exists("user:1") {
        Ok(exists) => nx_log!("Exists after delete: {}", exists),
        Err(e) => {
            nx_log!("Exists failed: {:?}", e);
            return;
        }
    }

    nx_log!("kv_get_set_delete: done");
}
