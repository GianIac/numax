use crate::ffi;

const ERR_INTERNAL: i32 = -3;

/// Log string to host.
/// Prefer v2 if available (returns i32).
pub fn log(s: &str) {
    unsafe {
        // If you exported only v2 on the host side, this must exist.
        // If you export both (recommended), this still works.
        let rc = ffi::host_log_v2(s.as_ptr() as u32, s.len() as u32);

        // Best-effort behavior: we don't hard-fail on logging errors.
        // You can optionally add debug behavior here.
        let _ = rc;
        if rc == ERR_INTERNAL {
            // ignore (best-effort)
        }
    }
}

/// Macro comoda (public API for guests)
#[macro_export]
macro_rules! nx_log {
    ($($arg:tt)*) => {{
        let msg = $crate::__alloc::format!($($arg)*);
        $crate::log::log(&msg);
    }};
}
