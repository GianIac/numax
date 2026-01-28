use crate::ffi;

pub fn log(s: &str) {
    unsafe { 
        ffi::host_log(s.as_ptr() as u32, s.len() as u32);
    }
}

/// Macro comoda
#[macro_export]
macro_rules! nx_log {
    ($($arg:tt)*) => {{
        let msg = $crate::__alloc::format!($($arg)*);
        $crate::log::log(&msg);
    }};
}
