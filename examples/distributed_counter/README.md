# Distributed Counter Example

Esempio di contatore distribuito che usa la sincronizzazione CRDT di Numax.

## Build

```bash
cd examples/distributed_counter
cargo build --release --target wasm32-unknown-unknown
```

## Run

### Nodo A (primo nodo)

```bash
nx run target/wasm32-unknown-unknown/release/distributed_counter.wasm \
    --listen 0.0.0.0:9000 \
    --sync-prefix "counter:" \
    --datastore-path ./data-a \
    -v
```

### Nodo B (si connette ad A)

```bash
nx run target/wasm32-unknown-unknown/release/distributed_counter.wasm \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --sync-prefix "counter:" \
    --datastore-path ./data-b \
    -v
```

## Risultato Atteso

Eseguendo più volte su entrambi i nodi, il contatore converge:
- Ogni nodo incrementa il proprio contatore locale
- Le operazioni vengono replicate via CRDT
- Alla fine, entrambi i nodi vedono lo stesso valore totale

## Note

- Il prefisso `counter:` indica che le chiavi che iniziano con "counter:" sono replicate
- Ogni nodo ha il proprio datastore (`./data-a`, `./data-b`)
- Il flag `-v` abilita il logging verbose per vedere la sincronizzazione