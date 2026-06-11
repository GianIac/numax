---
title: CLI
description: Riferimento per l'interfaccia a riga di comando `nx`.
---

`nx` è l'interfaccia a riga di comando di Numax. Ha due comandi:

- `nx run` - carica ed esegue un modulo WASM
- `nx config` - gestisce i file di configurazione

---

## nx run

```
nx run <MODULE> [OPTIONS]
```

Carica `<MODULE>`, avvia il runtime, chiama `run()` una volta, poi termina.
Se `--listen` è passato, la sync è abilitata e il runtime resta attivo
finché `--settle-for` non scade, oppure fino a SIGINT/SIGTERM se non è impostata una finestra di settle.

### Obbligatorio

| Argomento | Descrizione |
|---|---|
| `<MODULE>` | Percorso al file `.wasm` da eseguire |

### Storage

| Flag | Env | Descrizione |
|---|---|---|
| `--datastore-path <PATH>` | `NX_DATASTORE_PATH` | Directory per il datastore sled locale. Default: `./nx-data` |
| `--config <PATH>` | - | Percorso a un file di configurazione TOML Numax |

### Networking

La sync è disabilitata per default. Passa `--listen` per abilitarla.

| Flag | Env | Descrizione |
|---|---|---|
| `--listen <ADDR>` | `NX_LISTEN` | Indirizzo su cui ascoltare (es. `0.0.0.0:9000`). Obbligatorio per la sync |
| `--peer <ADDR>` | `NX_PEER` / `NX_PEERS` | Indirizzo di un peer a cui connettersi. Ripetibile. Richiede `--listen` |

`NX_PEERS` accetta una lista separata da virgole: `NX_PEERS=127.0.0.1:9001,127.0.0.1:9002`

### Timing

| Flag | Descrizione |
|---|---|
| `--wait-before-run <DURATION>` | Attendi dopo aver avviato la sync e prima di chiamare `run()`. Dà tempo ai peer di connettersi. Richiede `--listen` |
| `--settle-for <DURATION>` | Tieni la sync attiva per questa durata dopo il ritorno di `run()`, poi spegni. Richiede `--listen` |
| `--shutdown-timeout <DURATION>` | Tempo massimo per lo shutdown graceful prima di restituire un errore |

Formato delle durate: `500ms`, `5s`, `2m`, o un numero semplice (interpretato come secondi).

### Ispezione dello stato CRDT

Questi flag stampano il valore finale lato host di una chiave CRDT dopo che la finestra di settle è completata.
Tutti richiedono `--listen`.

| Flag | Tipo CRDT | Formato output |
|---|---|---|
| `--print-gcounter <KEY>` | GCounter | `key = 42` |
| `--print-pncounter <KEY>` | PNCounter | `key = -3` |
| `--print-lww-register <KEY>` | LWW-Register | `key = value` oppure `key = <unset>` |
| `--print-lww-map <KEY>` | LWW-Map | `key = {field1=val1, field2=val2}` |
| `--print-orset <KEY>` | ORSet | `key = [tag1, tag2]` |
| `--print-rga <KEY>` | RGA | `key = [item1, item2]` |

### Logging

| Flag | Env | Valori | Descrizione |
|---|---|---|---|
| `-v` / `--verbose` | - | - | Imposta il log level a `debug` |
| `--log-level <LEVEL>` | `NX_LOG_LEVEL` | `trace` `debug` `info` `warn` `error` | Log level esplicito. Ha la precedenza su `--verbose` |
| `--log-format <FORMAT>` | `NX_LOG_FORMAT` | `text` `json` | Formato di output per i log del runtime. Default: `text` |

### Osservabilità

| Flag | Env | Descrizione |
|---|---|---|
| `--observability-listen <ADDR>` | `NX_OBSERVABILITY_LISTEN` | Espone un endpoint HTTP locale per le metriche (es. `127.0.0.1:9100`) |

### TLS / mTLS

TLS è opzionale. Per abilitarlo, fornisci `--tls-cert` e `--tls-key` insieme.
Per mTLS (autenticazione reciproca), fornisci anche `--tls-ca`.

| Flag | Env | Descrizione |
|---|---|---|
| `--tls-cert <PATH>` | `NX_TLS_CERT` | Percorso al certificato TLS di questo nodo (PEM) |
| `--tls-key <PATH>` | `NX_TLS_KEY` | Percorso alla chiave privata TLS di questo nodo (PEM) |
| `--tls-ca <PATH>` | `NX_TLS_CA` | Percorso al certificato CA per verificare i peer (PEM). Abilita mTLS |
| `--allowed-peers <ID1,ID2,...>` | `NX_ALLOWED_PEERS` | Allowlist di NodeId dei peer (hex), separati da virgola. Richiede `--tls-ca` |
| `--tls-insecure` | `NX_TLS_INSECURE` | Salta la verifica del certificato TLS. **Solo sviluppo. Non usare in produzione.** |

Regole:
- `--tls-cert` e `--tls-key` devono sempre essere forniti insieme.
- `--tls-insecure` è mutualmente esclusivo con `--tls-ca` e `--allowed-peers`.
- `--allowed-peers` richiede `--tls-ca`.

### Debug

| Flag | Descrizione |
|---|---|
| `--debug-protocol` | Usa JSON invece di bincode per il protocollo wire della sync. Utile per ispezionare il traffico con un packet capture. Richiede `--listen` |

### Precedenza della configurazione

Quando un flag ha una variabile d'ambiente corrispondente e una voce nel file TOML,
l'ordine di risoluzione è:

```
CLI flags > variabili d'ambiente NX_* > file di configurazione TOML > default del runtime
```

---

## nx config

### nx config init

```
nx config init [--output <PATH>] [--force]
```

Genera un file di configurazione TOML Numax commentato con tutti i campi disponibili e i loro default.

| Flag | Default | Descrizione |
|---|---|---|
| `--output <PATH>` | `numax.toml` | Dove scrivere il file |
| `--force` | - | Sovrascrive il file se esiste già |

Esempio:

```bash
nx config init --output node-a.toml
```

Il file generato ha questo aspetto:

```toml
# Numax configuration file.
# Precedence: CLI flags > NX_* environment variables > this file > defaults.

[storage]
datastore_path = "./nx-data"

[network]
listen = "0.0.0.0:9000"
peers = []
serialization_format = "bincode"

[tls]
# cert = "./certs/node.pem"
# key = "./certs/node-key.pem"
# ca = "./certs/ca.pem"
allowed_peers = []
insecure = false

[observability]
# listen = "127.0.0.1:9100"
log_level = "info"
log_format = "text"
request_timeout_secs = 5

[limits]
max_peers = 64
queued_ops_limit = 10000
op_log_limit = 10000
seen_ops_limit = 100000
max_message_size = "16MiB"
socket_timeout_secs = 30
reconnect_initial_delay = "500ms"
reconnect_max_delay = "30s"
peer_dead_after_failures = 3
anti_entropy_interval = "30s"

[discovery]
mode = "static"
```

### nx config validate

```
nx config validate [--config <PATH>]
```

Analizza e valida un file di configurazione TOML senza eseguire un modulo.
Esce con un codice non-zero e un messaggio di errore se il file non è valido.

| Flag | Default | Descrizione |
|---|---|---|
| `--config <PATH>` | `numax.toml` | Percorso al file di configurazione da validare |

Esempio:

```bash
nx config validate --config node-a.toml
# configuration is valid: node-a.toml
```

### nx config show

```
nx config show --config <PATH> --effective
```

Risolve e stampa la configurazione effettiva dopo aver applicato CLI flags, variabili
d'ambiente, file di configurazione e default del runtime - in quest'ordine.

| Flag | Default | Descrizione |
|---|---|---|
| `--config <PATH>` | `numax.toml` | Percorso al file di configurazione |
| `--effective` | - | Obbligatorio. Stampa la configurazione completamente risolta |

Esempio:

```bash
nx config show --config node-a.toml --effective
```

---

## Riferimento file di configurazione TOML

Un file di configurazione può fornire qualsiasi sottoinsieme delle sezioni seguenti.
Tutti i campi sono opzionali. I campi sconosciuti vengono rifiutati.

### [storage]

```toml
[storage]
datastore_path = "./nx-data"
```

| Campo | Tipo | Descrizione |
|---|---|---|
| `datastore_path` | percorso | Directory per il datastore sled locale |

### [network]

```toml
[network]
listen = "0.0.0.0:9000"
peers = ["127.0.0.1:9001"]
serialization_format = "bincode"
```

| Campo | Tipo | Valori | Descrizione |
|---|---|---|---|
| `listen` | stringa | `host:porta` | Indirizzo su cui ascoltare. Obbligatorio per la sync |
| `peers` | stringa[] | `host:porta` | Indirizzi dei peer a cui connettersi |
| `serialization_format` | stringa | `bincode` `json` | Formato wire. Default: `bincode` |

### [tls]

```toml
[tls]
cert = "./certs/node.pem"
key = "./certs/node-key.pem"
ca = "./certs/ca.pem"
allowed_peers = ["node-a", "node-b"]
insecure = false
```

| Campo | Tipo | Descrizione |
|---|---|---|
| `cert` | percorso | Certificato del nodo (PEM) |
| `key` | percorso | Chiave privata del nodo (PEM) |
| `ca` | percorso | Certificato CA per la verifica dei peer (PEM). Abilita mTLS |
| `allowed_peers` | stringa[] | Allowlist di NodeId dei peer |
| `insecure` | bool | Salta la verifica. Solo sviluppo |

### [observability]

```toml
[observability]
listen = "127.0.0.1:9100"
log_level = "info"
log_format = "text"
request_timeout_secs = 5
```

| Campo | Tipo | Valori | Descrizione |
|---|---|---|---|
| `listen` | stringa | `host:porta` | Indirizzo endpoint HTTP per le metriche |
| `log_level` | stringa | `trace` `debug` `info` `warn` `error` | Verbosità dei log |
| `log_format` | stringa | `text` `json` | Formato di output dei log |
| `request_timeout_secs` | intero | > 0 | Timeout delle richieste di osservabilità in secondi |

### [limits]

```toml
[limits]
max_peers = 64
queued_ops_limit = 10000
op_log_limit = 10000
seen_ops_limit = 100000
max_message_size = "16MiB"
socket_timeout_secs = 30
reconnect_initial_delay = "500ms"
reconnect_max_delay = "30s"
peer_dead_after_failures = 3
anti_entropy_interval = "30s"
```

| Campo | Tipo | Descrizione |
|---|---|---|
| `max_peers` | intero | Numero massimo di peer connessi |
| `queued_ops_limit` | intero | Massimo di operazioni in coda per il broadcast |
| `op_log_limit` | intero | Massimo di operazioni nel log locale |
| `seen_ops_limit` | intero | Massimo di ID operazione tracciati per la deduplicazione |
| `max_message_size` | stringa | Dimensione massima del messaggio di sync (es. `16MiB`, `4KiB`) |
| `socket_timeout_secs` | intero | Timeout lettura/scrittura socket in secondi |
| `reconnect_initial_delay` | durata | Backoff iniziale prima di riconnettersi a un peer |
| `reconnect_max_delay` | durata | Backoff massimo per i tentativi di riconnessione |
| `peer_dead_after_failures` | intero | Fallimenti consecutivi prima che un peer venga marcato come morto |
| `anti_entropy_interval` | durata | Intervallo tra i cicli di riparazione anti-entropy |

`reconnect_initial_delay` e `reconnect_max_delay` devono essere forniti insieme.
`reconnect_initial_delay` deve essere minore o uguale a `reconnect_max_delay`.

### [discovery]

```toml
[discovery]
mode = "static"
```

| Campo | Tipo | Valori | Descrizione |
|---|---|---|---|
| `mode` | stringa | `static` | Modalità di scoperta dei peer. Solo `static` è supportato oggi. La scoperta dinamica è nel roadmap |

---

## Esempio completo a due nodi

```bash
# Genera i file di configurazione
nx config init --output node-a.toml --force
nx config init --output node-b.toml --force

# Modifica node-a.toml: imposta listen = "0.0.0.0:9000", peers = ["127.0.0.1:9001"]
# Modifica node-b.toml: imposta listen = "0.0.0.0:9001", peers = ["127.0.0.1:9000"]

# Valida
nx config validate --config node-a.toml
nx config validate --config node-b.toml

# Ispeziona la configurazione risolta
nx config show --config node-a.toml --effective

# Esegui
nx run my_module.wasm --config node-a.toml --settle-for 5s --print-gcounter counter:visits
nx run my_module.wasm --config node-b.toml --settle-for 5s --print-gcounter counter:visits
```