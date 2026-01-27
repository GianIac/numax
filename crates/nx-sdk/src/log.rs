use crate::ffi;

/// Log semplice (safe wrapper).
pub fn log(s: &str) {
    unsafe { ffi::host_log(s.as_ptr() as i32, s.len() as i32) }
}

/// Macro comoda
#[macro_export]
macro_rules! nx_log {
    ($($arg:tt)*) => {{
        let msg = $crate::__alloc::format!($($arg)*);
        $crate::log::log(&msg);
    }};
}
