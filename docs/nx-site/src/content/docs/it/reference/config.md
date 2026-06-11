---
title: Configurazione
description: Riferimento per `numax.toml` e override ambiente.
---

Numax risolve la sua configurazione runtime da quattro sorgenti, applicate in quest'ordine:

```
CLI flags  >  variabili d'ambiente NX_*  >  numax.toml  >  default del runtime
```

Una sorgente successiva riempie solo ciò che le precedenti hanno lasciato non impostato.
Puoi eseguire un singolo nodo con soli CLI flag, o descrivere un cluster completo
con un file TOML e sovrascrivere singoli campi al momento dell'avvio.

---

## Generare un file di configurazione

```bash
nx config init --output numax.toml
```

Scrive un file completamente commentato con tutti i campi disponibili e i loro default.
Passa `--force` per sovrascrivere un file esistente.

Per ispezionare cosa userà effettivamente il runtime dopo aver unito tutte le sorgenti:

```bash
nx config show --config numax.toml --effective
```

Per validare un file senza eseguire un modulo:

```bash
nx config validate --config numax.toml
```

---

## File di default completo

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

Tutti i campi sono opzionali. I campi sconosciuti vengono rifiutati in fase di validazione.

---

## [storage]

Posizione del datastore locale. Lo store è un database embedded sled.

| Campo | Tipo | Default | Descrizione |
|---|---|---|---|
| `datastore_path` | percorso | `./nx-data` | Directory dove viene scritto il datastore sled locale |

Il datastore persiste tra le esecuzioni. Ogni nodo deve usare la propria directory.
Per ripartire da zero, elimina la directory prima di eseguire.

```toml
[storage]
datastore_path = "./data/node-a"
```

---

## [network]

Controlla se la sync è abilitata e a chi connettersi.
La sync è disabilitata quando questa sezione è assente e nessun flag CLI/env fornisce un indirizzo di ascolto.

| Campo | Tipo | Default | Descrizione |
|---|---|---|---|
| `listen` | stringa | — | Indirizzo su cui ascoltare, es. `0.0.0.0:9000`. Obbligatorio per abilitare la sync |
| `peers` | stringa[] | `[]` | Indirizzi dei peer a cui connettersi, es. `["127.0.0.1:9001"]` |
| `serialization_format` | stringa | `bincode` | Formato wire: `bincode` (produzione) o `json` (ispezione debug) |

```toml
[network]
listen = "0.0.0.0:9000"
peers = ["127.0.0.1:9001", "127.0.0.1:9002"]
serialization_format = "bincode"
```

---

## [tls]

Configurazione TLS e mTLS opzionale. Se questa sezione è assente, le connessioni sono non cifrate.

Per abilitare TLS, fornisci `cert` e `key`.
Per abilitare mTLS (autenticazione reciproca), fornisci anche `ca`.

| Campo | Tipo | Default | Descrizione |
|---|---|---|---|
| `cert` | percorso | — | Certificato TLS di questo nodo (PEM) |
| `key` | percorso | — | Chiave privata TLS di questo nodo (PEM) |
| `ca` | percorso | — | Certificato CA per verificare i peer (PEM). Abilita mTLS |
| `allowed_peers` | stringa[] | `[]` | Allowlist di NodeId dei peer (hex). Richiede `ca` |
| `insecure` | bool | `false` | Salta la verifica del certificato TLS. **Solo sviluppo. Non usare in produzione** |

Regole:
- `cert` e `key` devono essere forniti insieme.
- `insecure` è mutualmente esclusivo con `ca` e `allowed_peers`.
- `allowed_peers` richiede `ca`.

```toml
[tls]
cert = "./certs/node-a.pem"
key = "./certs/node-a-key.pem"
ca = "./certs/ca.pem"
allowed_peers = ["node-b-id-hex", "node-c-id-hex"]
insecure = false
```

---

## [observability]

Endpoint HTTP opzionale per metriche e configurazione dei log.

| Campo | Tipo | Default | Descrizione |
|---|---|---|---|
| `listen` | stringa | — | Indirizzo per esporre l'endpoint HTTP delle metriche, es. `127.0.0.1:9100` |
| `log_level` | stringa | `info` | Verbosità log: `trace`, `debug`, `info`, `warn`, `error` |
| `log_format` | stringa | `text` | Formato output log: `text` o `json` |
| `request_timeout_secs` | intero | `5` | Timeout richieste HTTP osservabilità in secondi. Deve essere > 0 |

```toml
[observability]
listen = "127.0.0.1:9100"
log_level = "debug"
log_format = "json"
request_timeout_secs = 5
```

---

## [limits]

Controllo granulare sul comportamento della sync e sui limiti delle risorse.
Si applicano solo quando la sync è abilitata. I default sono conservativi e adatti
alla maggior parte dei setup multi-nodo su singola macchina.

| Campo | Tipo | Default | Descrizione |
|---|---|---|---|
| `max_peers` | intero | `64` | Numero massimo di peer connessi simultaneamente |
| `queued_ops_limit` | intero | `10000` | Massimo ops in coda per il broadcast prima del backpressure |
| `op_log_limit` | intero | `10000` | Massimo ops nel log locale per anti-entropy |
| `seen_ops_limit` | intero | `100000` | Massimo ID operazione tracciati per la deduplicazione |
| `max_message_size` | stringa | `16MiB` | Dimensione massima messaggio di sync. Accetta `KiB`, `MiB` o byte semplici |
| `socket_timeout_secs` | intero | `30` | Timeout lettura/scrittura socket in secondi. Deve essere > 0 |
| `reconnect_initial_delay` | durata | `500ms` | Backoff iniziale prima di riconnettersi a un peer perso |
| `reconnect_max_delay` | durata | `30s` | Tetto massimo del backoff per i tentativi di riconnessione |
| `peer_dead_after_failures` | intero | `3` | Fallimenti consecutivi prima che un peer venga marcato come morto |
| `anti_entropy_interval` | durata | `30s` | Intervallo tra i cicli di riparazione anti-entropy |

`reconnect_initial_delay` e `reconnect_max_delay` devono essere forniti insieme
e `reconnect_initial_delay` deve essere ≤ `reconnect_max_delay`.

Tutti i campi interi devono essere > 0.

```toml
[limits]
max_peers = 16
queued_ops_limit = 5000
op_log_limit = 5000
seen_ops_limit = 50000
max_message_size = "8MiB"
socket_timeout_secs = 15
reconnect_initial_delay = "250ms"
reconnect_max_delay = "15s"
peer_dead_after_failures = 5
anti_entropy_interval = "60s"
```

---

## [discovery]

Controlla come vengono scoperti i peer.

| Campo | Tipo | Default | Descrizione |
|---|---|---|---|
| `mode` | stringa | `static` | Modalità di scoperta. Solo `static` è supportato oggi |

In modalità `static`, i peer sono elencati esplicitamente in `[network].peers` o tramite i flag `--peer`.
La scoperta dinamica (mDNS, DNS-SRV, SWIM) è nel roadmap.

```toml
[discovery]
mode = "static"
```

---

## Variabili d'ambiente

Le variabili d'ambiente si trovano tra i CLI flag e il file TOML nella catena di precedenza.
Sono utili per segreti (percorsi TLS), ambienti container e CI.

| Variabile | Tipo | Campo equivalente | Descrizione |
|---|---|---|---|
| `NX_DATASTORE_PATH` | percorso | `[storage].datastore_path` | Directory datastore locale |
| `NX_LISTEN` | stringa | `[network].listen` | Indirizzo di ascolto sync |
| `NX_PEER` | stringa | `[network].peers` (singolo) | Indirizzo di un singolo peer |
| `NX_PEERS` | stringa | `[network].peers` (lista) | Lista peer separata da virgole |
| `NX_SERIALIZATION_FORMAT` | stringa | `[network].serialization_format` | `bincode` o `json` |
| `NX_TLS_CERT` | percorso | `[tls].cert` | Percorso certificato nodo |
| `NX_TLS_KEY` | percorso | `[tls].key` | Percorso chiave nodo |
| `NX_TLS_CA` | percorso | `[tls].ca` | Percorso certificato CA |
| `NX_ALLOWED_PEERS` | stringa | `[tls].allowed_peers` | Allowlist NodeId peer separata da virgole |
| `NX_TLS_INSECURE` | bool | `[tls].insecure` | `1`, `true`, `yes`, `on` / `0`, `false`, `no`, `off` |
| `NX_OBSERVABILITY_LISTEN` | stringa | `[observability].listen` | Indirizzo endpoint metriche |
| `NX_LOG_LEVEL` | stringa | `[observability].log_level` | `trace`, `debug`, `info`, `warn`, `error` |
| `NX_LOG_FORMAT` | stringa | `[observability].log_format` | `text` o `json` |

`NX_PEER` e `NX_PEERS` sono additivi: se entrambi sono impostati, entrambi i peer vengono usati.

---

## Formato durate

I campi durata nel file TOML e nei flag CLI accettano:

| Formato | Esempio | Significato |
|---|---|---|
| Millisecondi | `500ms` | 500 millisecondi |
| Secondi | `5s` | 5 secondi |
| Minuti | `2m` | 2 minuti |
| Numero semplice | `5` | 5 secondi |

Le durate zero vengono rifiutate.

---

## Pattern setup a due nodi

```toml
# node-a.toml
[storage]
datastore_path = "./data-a"

[network]
listen = "0.0.0.0:9000"
peers = ["127.0.0.1:9001"]
serialization_format = "bincode"

[limits]
anti_entropy_interval = "30s"

[discovery]
mode = "static"
```

```toml
# node-b.toml
[storage]
datastore_path = "./data-b"

[network]
listen = "0.0.0.0:9001"
peers = ["127.0.0.1:9000"]
serialization_format = "bincode"

[limits]
anti_entropy_interval = "30s"

[discovery]
mode = "static"
```

```bash
nx config validate --config node-a.toml
nx config validate --config node-b.toml

nx run my_module.wasm --config node-a.toml --settle-for 5s
nx run my_module.wasm --config node-b.toml --settle-for 5s
```

---

## Correlati

- [Riferimento CLI](/it/reference/cli/) - riferimento completo flag e sottocomandi
- [Host API](/it/reference/host-api/) - funzioni disponibili ai moduli WASM