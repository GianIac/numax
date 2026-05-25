use crate::ffi;

/// Current Unix timestamp in milliseconds.
pub fn now() -> u64 {
    unsafe { ffi::time_now() }
}

/// Monotonic milliseconds since the runtime process initialized its monotonic clock.
pub fn monotonic() -> u64 {
    unsafe { ffi::time_monotonic() }
}
