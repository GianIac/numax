use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicU32, AtomicUsize, Ordering};

use nx_sdk::crdt::{gcounter, lww_map, lww_register, orset, pncounter, rga};
use nx_sdk::{NxError, Result};

static RESPONSE: AtomicI32 = AtomicI32::new(-3);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static RGA_BUFFER_RETRY: AtomicUsize = AtomicUsize::new(0);
static FIRST_RGA_CAP: AtomicU32 = AtomicU32::new(0);
static SECOND_RGA_CAP: AtomicU32 = AtomicU32::new(0);
static TEST_LOCK: Mutex<()> = Mutex::new(());

macro_rules! mock_mutation {
    ($name:ident($($arg:ident: $ty:ty),*)) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $name($($arg: $ty),*) -> i32 {
            $(let _ = $arg;)*
            CALLS.fetch_add(1, Ordering::SeqCst);
            RESPONSE.load(Ordering::SeqCst)
        }
    };
}

mock_mutation!(crdt_gcounter_inc(key_ptr: u32, key_len: u32, delta: u64));
mock_mutation!(crdt_pncounter_inc(key_ptr: u32, key_len: u32, delta: u64));
mock_mutation!(crdt_pncounter_dec(key_ptr: u32, key_len: u32, delta: u64));
mock_mutation!(crdt_lww_set(key_ptr: u32, key_len: u32, value_ptr: u32, value_len: u32));
mock_mutation!(crdt_lww_map_set(key_ptr: u32, key_len: u32, field_ptr: u32, field_len: u32, value_ptr: u32, value_len: u32));
mock_mutation!(crdt_lww_map_remove(key_ptr: u32, key_len: u32, field_ptr: u32, field_len: u32));
mock_mutation!(crdt_orset_add(key_ptr: u32, key_len: u32, element_ptr: u32, element_len: u32));
mock_mutation!(crdt_orset_remove(key_ptr: u32, key_len: u32, element_ptr: u32, element_len: u32));
mock_mutation!(crdt_rga_delete(key_ptr: u32, key_len: u32, id_ptr: u32, id_len: u32));

#[unsafe(no_mangle)]
pub extern "C" fn crdt_rga_insert(
    _key_ptr: u32,
    _key_len: u32,
    _parent_ptr: u32,
    _parent_len: u32,
    _value_ptr: u32,
    _value_len: u32,
    _out_id_ptr: u32,
    out_id_cap: u32,
) -> i32 {
    let call = CALLS.fetch_add(1, Ordering::SeqCst);
    if call == 0 {
        FIRST_RGA_CAP.store(out_id_cap, Ordering::SeqCst);
    } else {
        SECOND_RGA_CAP.store(out_id_cap, Ordering::SeqCst);
    }
    if RGA_BUFFER_RETRY.load(Ordering::SeqCst) != 0 && call == 0 {
        -2
    } else {
        RESPONSE.load(Ordering::SeqCst)
    }
}

fn assert_one_call<T>(call: impl FnOnce() -> Result<T>, expected: NxError) {
    CALLS.store(0, Ordering::SeqCst);
    let result = call();
    assert!(matches!(result, Err(error) if error == expected));
    assert_eq!(CALLS.load(Ordering::SeqCst), 1);
}

#[test]
fn mutating_wrappers_do_not_retry_uncertain_errors() {
    let _guard = TEST_LOCK.lock().unwrap();
    for (response, expected) in [(-3, NxError::Internal), (-9, NxError::UnknownCode(-9))] {
        RESPONSE.store(response, Ordering::SeqCst);
        assert_one_call(|| gcounter::inc("key", 1), expected);
        assert_one_call(|| pncounter::inc("key", 1), expected);
        assert_one_call(|| pncounter::dec("key", 1), expected);
        assert_one_call(|| lww_register::set("key", b"value"), expected);
        assert_one_call(|| lww_map::set("key", "field", b"value"), expected);
        assert_one_call(|| lww_map::remove("key", "field"), expected);
        assert_one_call(|| orset::add("key", "element"), expected);
        assert_one_call(|| orset::remove("key", "element"), expected);
        assert_one_call(|| rga::insert_after("key", None, b"value"), expected);
        assert_one_call(|| rga::delete("key", "id"), expected);
    }
}

#[test]
fn rga_insert_retries_only_buffer_preflight_then_stops_on_uncertain_error() {
    let _guard = TEST_LOCK.lock().unwrap();
    RESPONSE.store(-3, Ordering::SeqCst);
    CALLS.store(0, Ordering::SeqCst);
    RGA_BUFFER_RETRY.store(1, Ordering::SeqCst);

    assert_eq!(
        rga::insert_after("key", None, b"value"),
        Err(NxError::Internal)
    );
    assert_eq!(CALLS.load(Ordering::SeqCst), 2);
    assert_eq!(FIRST_RGA_CAP.load(Ordering::SeqCst), 64);
    assert_eq!(SECOND_RGA_CAP.load(Ordering::SeqCst), 128);

    RGA_BUFFER_RETRY.store(0, Ordering::SeqCst);
}
