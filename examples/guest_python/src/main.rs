//! Minimal Numax Python guest implemented in RustPython.
//!
//! Compiled to a *core* WebAssembly module (wasm32-wasip1), not a
//! Component, so it loads via `wasmtime::Module`

use rustpython_vm::compiler::Mode;
use rustpython_vm::Interpreter;

mod nx_ffi {
    // import `host_log_v2` from the `nx` namespace.
    // strings are passed as: (pointer, length)
    // signature: (u32, u32) -> i32
    //
    // import `db_set` from the `nx` namespace.
    // writes a key/value pair into the embedded datastore.
    // signature: (u32, u32, u32, u32) -> i32
    #[link(wasm_import_module = "nx")]
    extern "C" {
        pub fn host_log_v2(ptr: *const u8, len: u32) -> i32;
        pub fn db_set(
            key_ptr: *const u8,
            key_len: u32,
            val_ptr: *const u8,
            val_len: u32,
        ) -> i32;
    }
}

/// The `nx` module as seen *inside* Python (`import nx; nx.log(...)`).
#[rustpython_vm::pymodule]
mod nx {
    use crate::nx_ffi;
    use rustpython_vm::builtins::PyStrRef;
    use rustpython_vm::{PyResult, VirtualMachine};

    #[pyfunction]
    fn log(msg: PyStrRef, vm: &VirtualMachine) -> PyResult<()> {
        let s = msg
            .to_str()
            .ok_or_else(|| vm.new_value_error("nx.log: string contains surrogates".to_owned()))?;
        let bytes = s.as_bytes();
        let rc = unsafe { nx_ffi::host_log_v2(bytes.as_ptr(), bytes.len() as u32) };
        if rc < 0 {
            return Err(vm.new_runtime_error(format!("nx.log failed (code {rc})")));
        }
        Ok(())
    }

    #[pyfunction]
    fn db_set(key: PyStrRef, value: PyStrRef, vm: &VirtualMachine) -> PyResult<i32> {
        let k = key
            .to_str()
            .ok_or_else(|| vm.new_value_error("nx.db_set: key contains surrogates".to_owned()))?
            .as_bytes();
        let v = value
            .to_str()
            .ok_or_else(|| vm.new_value_error("nx.db_set: value contains surrogates".to_owned()))?
            .as_bytes();
        let rc = unsafe { nx_ffi::db_set(k.as_ptr(), k.len() as u32, v.as_ptr(), v.len() as u32) };
        if rc < 0 {
            return Err(vm.new_runtime_error(format!("nx.db_set failed (code {rc})")));
        }
        Ok(rc)
    }
}

/// exported guest entrypoint expected by Numax.
#[no_mangle]
pub extern "C" fn run() {
    // Native modules must be registered on the builder *before* the
    // interpreter is built (rustpython-vm 0.5's InterpreterBuilder).
    let builder = Interpreter::builder(Default::default());
    let nx_def = nx::module_def(&builder.ctx);
    let interp = builder.add_native_module(nx_def).build();

    interp.enter(|vm| {
        let scope = vm.new_scope_with_builtins();

        let source = include_str!("guest.py");
        let code_obj = match vm.compile(source, Mode::Exec, "<guest.py>".to_owned()) {
            Ok(code) => code,
            Err(err) => {
                let exc = vm.new_syntax_error(&err, Some(source));
                vm.print_exception(exc);
                return;
            }
        };

        if let Err(exc) = vm.run_code_obj(code_obj, scope) {
            vm.print_exception(exc);
        }
    });
}

fn main() {}