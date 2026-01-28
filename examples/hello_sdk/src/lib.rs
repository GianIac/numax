use nx_sdk::log;

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    log("Hello Numax via SDK");
}
