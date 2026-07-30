#[cfg(not(test))]
use crate::ffi;

#[cfg(not(test))]
const ERR_INTERNAL: i32 = -3;

#[cfg(not(test))]
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

#[cfg(test)]
std::thread_local! {
    static TEST_MESSAGES: std::cell::RefCell<Vec<String>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

#[cfg(test)]
pub fn log(s: &str) {
    TEST_MESSAGES.with(|messages| messages.borrow_mut().push(s.to_owned()));
}

#[cfg(test)]
fn take_test_messages() -> Vec<String> {
    TEST_MESSAGES.with(|messages| core::mem::take(&mut *messages.borrow_mut()))
}

/// Logs a formatted message through the Numax host.
///
/// # Examples
///
/// ```
/// # #[cfg(target_arch = "wasm32")]
/// # fn main() {
/// use nx_sdk::nx_log;
///
/// nx_log!("hello");
///
/// let id = 42;
/// nx_log!("user {} created", id);
///
/// let (a, b) = ("source", "destination");
/// nx_log!("{} -> {}", a, b);
/// # }
/// # #[cfg(not(target_arch = "wasm32"))]
/// # fn main() {}
/// ```
#[macro_export]
macro_rules! nx_log {
    ($($arg:tt)*) => {{
        let msg = $crate::__alloc::format!($($arg)*);
        $crate::log::log(&msg);
    }};
}

#[cfg(test)]
mod tests {
    fn assert_logged_message(expected: &str, log_message: impl FnOnce()) {
        let _ = super::take_test_messages();
        log_message();
        assert_eq!(super::take_test_messages(), vec![expected.to_owned()]);
    }

    #[test]
    fn logs_plain_literal() {
        assert_logged_message("hello", || crate::nx_log!("hello"));
    }

    #[test]
    fn logs_formatted_message() {
        let id = 42;
        assert_logged_message("user 42 created", || {
            crate::nx_log!("user {} created", id);
        });
    }

    #[test]
    fn logs_multiple_format_arguments() {
        let (a, b) = ("source", "destination");
        assert_logged_message("source -> destination", || {
            crate::nx_log!("{} -> {}", a, b);
        });
    }
}
