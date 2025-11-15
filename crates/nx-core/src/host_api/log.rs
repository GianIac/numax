use anyhow::Result;
use wasmtime::{Caller, Linker};

pub fn add_to_linker<T: 'static>(linker: &mut Linker<T>) -> Result<()> {
    linker.func_wrap(
        "nx",
        "host_log",
        move |mut caller: Caller<'_, T>, ptr: i32, len: i32| {
            let memory = match caller.get_export("memory") {
                Some(wasmtime::Extern::Memory(mem)) => mem,
                _ => {
                    eprintln!("[nx-core] host_log: no memory export");
                    return;
                }
            };

            let mut buf = vec![0u8; len as usize];
            if let Err(e) = memory.read(&caller, ptr as usize, &mut buf) {
                eprintln!("[nx-core] host_log: memory read error: {e}");
                return;
            }

            if let Ok(s) = String::from_utf8(buf) {
                println!("[guest] {s}");
            } else {
                println!("[guest] <non-utf8-bytes>");
            }
        },
    )?;

    Ok(())
}
