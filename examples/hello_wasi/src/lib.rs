use std::env;

#[unsafe(no_mangle)]
pub extern "C" fn _start() {
    println!("Hello from WASI via Numax!");

    let args: Vec<String> = env::args().collect();
    println!("Args seen by WASI module: {args:?}");
}
