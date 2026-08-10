use crate::ffi;

/// Current Unix timestamp in milliseconds.
#[inline]
pub fn now() -> u64 {
    unsafe { ffi::time_now() }
}

/// Monotonic milliseconds since the runtime process initialized its monotonic clock.
#[inline]
pub fn monotonic() -> u64 {
    unsafe { ffi::time_monotonic() }
}
