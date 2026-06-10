---
title: "Quickstart: 5 minuti"
description: Esegui un esempio distribuito Numax da zero.
---

## Cosa stiamo per fare

Lo facciamo insieme: Due moduli. Due nodi. Un contatore.

Li avvii sulla stessa macchina. Si trovano, si scambiano un incremento,
e concordano entrambi che il contatore vale **2**.

Tu scrivi **zero righe di networking**. Ci pensa Numax.

---

## Cosa ti serve

- [Rust](https://rustup.rs/) con il target `wasm32-unknown-unknown`
- Git

```bash
rustup target add wasm32-unknown-unknown
```

Basta così.

---

## Step 1 - Prendi Numax

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo build --release
```

Se vuoi, puoi impostare `nx` come variabile d'ambiente così ogni comando resta corto:

```bash
export NX=./target/release/nx
```

> Da ora ogni comando usa `$NX`.

---

## Step 2 - Compila l'esempio

```bash
cd examples/distributed_counter
cargo build --release --target wasm32-unknown-unknown
cd ../..
```

Ora hai un modulo `.wasm` in:
```
examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm
```

Un contatore che si incrementa di 1 ogni volta che gira.

---

## Step 3 - Avvia due nodi

Apri **due terminali** nella cartella `numax/`.
In **entrambi** imposta le variabili:

```bash
export NX=./target/release/nx
export WASM=examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm
```

**Terminale 1 - Nodo A:**

```bash
$NX run $WASM \
    --listen 0.0.0.0:9000 \
    --peer 127.0.0.1:9001 \
    --datastore-path ./data-a \
    --wait-before-run 1500ms \
    --settle-for 5s \
    --print-gcounter counter:visits \
    -v
```

**Terminale 2 - Nodo B:**

```bash
$NX run $WASM \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --datastore-path ./data-b \
    --wait-before-run 1500ms \
    --settle-for 5s \
    --print-gcounter counter:visits \
    -v
```

Avviali entro pochi secondi l'uno dall'altro.

---

## Step 4 - Guarda la convergenza

Dopo ~6 secondi, **entrambi i terminali** stampano:

```text
counter:visits = 2
```

Nodo A ha incrementato di 1. Nodo B ha incrementato di 1. Si sono trovati,
si sono scambiati lo stato, e sono convergiti alla verità - **senza che tu faccia niente**.

---

## Cosa è successo?

```
Nodo A                             Nodo B
  |                                  |
  +-- incrementa → slot locale = 1   +-- incrementa → slot locale = 1
  |                                  |
  +-- invia a B ──────────────────>  +-- riceve lo slot di A
  |                                  |
  +<────────────────── invia ad A ───+
  |                                  |
  +-- merge: somma(1, 1) = 2         +-- merge: somma(1, 1) = 2
  |                                  |
"counter:visits = 2"             "counter:visits = 2"
```

Questo è un **GCounter** - un CRDT grow-only. Ogni nodo possiede il proprio slot.
Il totale è la somma di tutti gli slot. Il merge è semplicemente prendere il
massimo per slot. Nessun coordinatore. Nessun conflitto. Converge sempre.

Il tuo modulo `.wasm` ha chiamato esattamente una funzione:

```rust
gcounter::inc("counter:visits", 1);
```

Tutto il resto - networking, sincronizzazione, persistenza, merge - era Numax.

---

## Pulisci e ricomincia

```bash
rm -rf ./data-a ./data-b
```

Lo stato del GCounter è persistente. Senza rimuovere le cartelle dati,
la prossima esecuzione riparte da dove si era fermata.

Adesso prova ad aggiungere altri nodi, passa da 2 a 3 o a 6! Aggiungi altri `--peer` flag e guardali convergere tutti.

---

## Vuoi sbizzarrirti?

Se vuoi andare oltre, la [directory degli esempi](https://github.com/GianIac/numax/tree/main/examples)
ha tutto - scegli uno, leggi 100 righe di Rust, e capisci esattamente cosa fa Numax:

| Esempio | Cosa dimostra |
|---|---|
| [`distributed_counter`](https://github.com/GianIac/numax/tree/main/examples/distributed_counter) | GCounter - contatore grow-only replicato |
| [`distributed_status`](https://github.com/GianIac/numax/tree/main/examples/distributed_status) | LWW-Register - vince l'ultimo che scrive |
| [`distributed_settings`](https://github.com/GianIac/numax/tree/main/examples/distributed_settings) | LWW-Map - mappa di configurazione replicata |
| [`distributed_tags`](https://github.com/GianIac/numax/tree/main/examples/distributed_tags) | ORSet - aggiungi/rimuovi tag, zero conflitti |
| [`distributed_comments`](https://github.com/GianIac/numax/tree/main/examples/distributed_comments) | RGA - stream di commenti ordinati e replicati |
| [`distributed_inventory`](https://github.com/GianIac/numax/tree/main/examples/distributed_inventory) | rifornimento / vendita / reso su uno SKU condiviso |
| [`distributed_chat`](https://github.com/GianIac/numax/tree/main/examples/distributed_chat) | chat locale con l'API key-value |