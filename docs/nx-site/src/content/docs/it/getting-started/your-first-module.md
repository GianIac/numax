---
title: Il tuo primo modulo
description: Scrivi ed esegui un modulo WASM minimale.
---

## Cos'è un modulo Numax?

Un modulo Numax è un file `.wasm` con una funzione esportata chiamata `run`.
Quando esegui `nx run your_module.wasm`, il runtime carica il file,
collega la host API, e chiama `run()` una volta. Storage, networking, sync - tutto opzionale, tutto nelle tue mani.

Qualsiasi linguaggio che compila in WASM può essere un modulo Numax. Questa pagina mostra
Rust (con e senza SDK), C, C++, e un'anteprima di come saranno Go e Python.

---

## Cosa ti serve

Numax già compilato dal [Quickstart](/it/getting-started/quickstart-5-min/).
Se non l'hai fatto:

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo build --release
export NX=./target/release/nx
```

---

## Rust - con nx-sdk

Il modo raccomandato. L'SDK avvolge tutte le import raw dell'host in funzioni Rust normali.

### Step 1 - Crea il crate

```bash
cargo new --lib my_module
cd my_module
```

Apri `Cargo.toml` e sostituisci tutto con:

```toml
[package]
name = "my_module"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
nx-sdk = { path = "../crates/nx-sdk" }

[profile.release]
lto = true
opt-level = "z"
codegen-units = 1
panic = "abort"

[workspace]
```

Due cose da notare:

- `crate-type = ["cdylib"]` - dice a Rust di produrre un file `.wasm` invece di una libreria
  Rust normale. Senza questo, `cargo build` produce un `.rlib` che Numax non può caricare.
- `nx-sdk = { path = "../crates/nx-sdk" }` - l'SDK guest. Avvolge le import raw dell'host
  (`db_get`, `host_log_v2`, `gcounter_inc`, ...) in funzioni Rust normali, così non devi
  mai toccare FFI direttamente.

### Step 2 - Scrivi il modulo

Apri `src/lib.rs` e sostituisci tutto con:

```rust
use nx_sdk::log;

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    log("Ciao dal mio primo modulo Numax!");
}
```

Riga per riga:

- `use nx_sdk::log` - importa la funzione `log` dall'SDK. Internamente chiama `host_log_v2`
  sull'host Numax, che stampa sul terminale.
- `#[unsafe(no_mangle)]` - dice al compilatore Rust di mantenere il nome della funzione
  così com'è nel `.wasm` compilato. Senza questo, Rust manglerebbe il nome
  (es. `_ZN9my_module3runE`) e Numax non troverebbe l'entry point.
- `pub extern "C"` - esporta la funzione con la C ABI così WASM la espone correttamente.
- `run()` - il nome che Numax cerca. Puoi mettere qualsiasi logica dentro.

### Step 3 - Compila ed esegui

```bash
cargo build --release --target wasm32-unknown-unknown
cd ..
export WASM=my_module/target/wasm32-unknown-unknown/release/my_module.wasm
$NX run $WASM
```

`wasm32-unknown-unknown` è il target Rust per WASM puro - niente OS, niente WASI, solo un
binario `.wasm`. Numax fornisce le funzioni host a runtime.

Output:

```text
[guest] Ciao dal mio primo modulo Numax!
```

Il prefisso `[guest]` viene aggiunto dall'host per distinguere i log del modulo dai log del runtime.

---

## Rust - log formattati e storage locale

### Log formattati

La funzione `log()` accetta una `&str` semplice. Per output formattato usa `nx_log!` - funziona
esattamente come `println!` ma passa per l'host:

```rust
use nx_sdk::{log, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    log("Modulo avviato.");
    nx_log!("1 + 1 = {}", 1 + 1);
}
```

### Storage locale

Leggi e scrivi dati persistenti con `nx_sdk::db`. I dati scritti qui vivono nel datastore
locale del nodo (sled su disco) e **non vengono replicati** - ogni nodo ha la sua copia.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // Scrivi un valore
    db::set("my_key", b"ciao numax").unwrap();

    // Rileggilo
    match db::get("my_key") {
        Ok(Some(bytes)) => nx_log!("Letto: {}", String::from_utf8_lossy(&bytes)),
        Ok(None)        => nx_log!("Chiave non trovata."),
        Err(e)          => nx_log!("Errore: {:?}", e),
    }

    // Cancellalo
    db::delete("my_key").unwrap();
}
```

Il datastore persiste tra un'esecuzione e l'altra. Per ripartire da zero, elimina la cartella
del datastore (default: `./nx-data`). Per dati che devono essere consistenti su più nodi,
usa `nx_sdk::crdt::*` invece di `db::*`.

Esempio completo: [`hello_sdk`](https://github.com/GianIac/numax/tree/main/examples/hello_sdk)

---

## C

Niente SDK. Importi le funzioni host manualmente tramite il namespace `nx` usando
`__attribute__((import_module))` e `__attribute__((import_name))`.
Le stringhe vengono passate come coppie puntatore + lunghezza.

Ti serve `clang` con supporto al target WASM. Su macOS: `brew install llvm`. Su Linux: `apt install clang`.

```c
__attribute__((import_module("nx")))
__attribute__((import_name("host_log_v2")))
extern int host_log_v2(const char* ptr, int len);

__attribute__((import_module("nx")))
__attribute__((import_name("db_set")))
extern int db_set(
    const char* key_ptr, int key_len,
    const char* val_ptr, int val_len
);

__attribute__((export_name("run")))
void run() {
    const char msg[] = "Hello from C guest!";
    host_log_v2(msg, sizeof(msg) - 1);

    const char* key = "hello";
    const char* val = "numax";
    db_set(key, 5, val, 5);

    const char done[] = "db_set ok";
    host_log_v2(done, sizeof(done) - 1);
}
```

Build:

```bash
clang \
  --target=wasm32-wasip1 \
  -O3 \
  -nostdlib \
  -Wl,--no-entry \
  -Wl,--export=run \
  -Wl,--allow-undefined \
  -o guest.wasm \
  src/guest.c
```

Esegui:

```bash
$NX run ./guest.wasm
```

Output:

```text
[guest] Hello from C guest!
[guest] db_set ok
```

Esempio completo: [`guest_c`](https://github.com/GianIac/numax/tree/main/examples/guest_c)

---

## C++

Come C, ma C++ manglerebbe i nomi delle funzioni internamente. Usa `export_name("run")` per
mantenere il simbolo WASM esportato stabile indipendentemente da quello che fa il compilatore.

```cpp
__attribute__((import_module("nx")))
__attribute__((import_name("host_log_v2")))
extern int host_log_v2(const char* ptr, int len);

__attribute__((import_module("nx")))
__attribute__((import_name("db_set")))
extern int db_set(
    const char* key_ptr, int key_len,
    const char* val_ptr, int val_len
);

__attribute__((export_name("run")))
void run() {
    const char msg[] = "Hello from C++ guest!";
    host_log_v2(msg, sizeof(msg) - 1);

    const char key[] = "hello";
    const char val[] = "numax-cpp";
    db_set(key, sizeof(key) - 1, val, sizeof(val) - 1);

    const char done[] = "db_set ok";
    host_log_v2(done, sizeof(done) - 1);
}
```

Build:

```bash
clang++ \
  --target=wasm32-wasip1 \
  -O3 \
  -nostdlib \
  -Wl,--no-entry \
  -Wl,--export=run \
  -Wl,--allow-undefined \
  -o guest.wasm \
  src/guest.cpp
```

Output:

```text
[guest] Hello from C++ guest!
[guest] db_set ok
```

Esempio completo: [`guest_cpp`](https://github.com/GianIac/numax/tree/main/examples/guest_cpp)

---

## Go (anteprima)

Go può compilare in WASM tramite `GOARCH=wasm GOOS=wasip1`. Lo stesso contratto si applica:
esporta una funzione chiamata `run`, importa le funzioni host dal namespace `nx`.

```go
//go:build wasm

package main

import "unsafe"

//go:wasmimport nx host_log_v2
func hostLog(ptr *byte, len int32) int32

func logStr(s string) {
    b := []byte(s)
    hostLog(&b[0], int32(len(b)))
}

//go:export run
func run() {
    logStr("Hello from Go guest!")
}

func main() {}
```

Build:

```bash
GOARCH=wasm GOOS=wasip1 go build -o guest.wasm main.go
$NX run ./guest.wasm
```

> Esempio Go completo in arrivo.

---

## Python (anteprima)

Python può targetizzare WASM tramite [py2wasm](https://wasmer.io/posts/py2wasm-a-python-to-wasm-compiler).
Il contratto host è lo stesso: una funzione `run` esportata, import dell'host dal namespace `nx`.

```python
# Concettuale - esempio completo in arrivo

def run():
    log("Hello from Python guest!")
```

> Esempio Python completo in arrivo.

---

## Altri esempi in arrivo

Go, Python, AssemblyScript, Zig - se compila in WASM, gira su Numax.
Altri esempi sono in arrivo.

Nel frattempo, sfoglia tutto quello già disponibile nella
[directory degli esempi](https://github.com/GianIac/numax/tree/main/examples).

---

## Ricompila dopo ogni modifica

```bash
# Rust
cargo build --release --target wasm32-unknown-unknown

# C / C++
./build.sh  # oppure build.bat su Windows

$NX run $WASM
```

---

## Altri esempi in arrivo

Go, Python, AssemblyScript, Zig - se compila in WASM, gira su Numax.
Altri esempi sono in arrivo.

Se vuoi richiedere un esempio o contribuirne uno direttamente, apri una issue o una PR -
è un piacere.

Nel frattempo, sfoglia tutto quello già disponibile nella
[directory degli esempi](https://github.com/GianIac/numax/tree/main/examples).

---
## Prossimi passi

- Rendilo distribuito - [Quickstart: 5 minuti](/it/getting-started/quickstart-5-min/)
- Esplora l'SDK completo: `nx_sdk::crdt`, `nx_sdk::net`, `nx_sdk::system`, `nx_sdk::time`
- Sfoglia la [directory degli esempi](https://github.com/GianIac/numax/tree/main/examples)