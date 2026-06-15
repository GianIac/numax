---
title: Installazione
description: Installa Numax su Linux, macOS, Windows o con Cargo.
---

Numax installa la CLI `nx`.

Per `v0.1.0`, il percorso consigliato è:

1. scaricare un binario precompilato dalla GitHub Release;
2. oppure compilare/installare da sorgente con Cargo.

Installazioni via package manager come Homebrew, winget e crates.io non sono ancora disponibili.

---

## Requisiti

Per la CLI precompilata:

- Linux, macOS o Windows;
- un terminale;
- `curl` o un browser per scaricare l'asset della release.

Per compilare moduli ed esempi:

- [Rust](https://rustup.rs/);
- il target `wasm32-unknown-unknown`.

```bash
rustup target add wasm32-unknown-unknown
```

---

## Linux

Usa la build Linux x86_64 musl:

```bash
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-musl
ARCHIVE="numax-${VERSION}-${TARGET}.tar.gz"

curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/${ARCHIVE}"
curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/SHA256SUMS"

grep " ${ARCHIVE}$" SHA256SUMS | sha256sum -c -
tar -xzf "${ARCHIVE}"
sudo install -m 755 "numax-${VERSION}-${TARGET}/nx" /usr/local/bin/nx

nx --version
```

Per Linux ARM64, usa `TARGET=aarch64-unknown-linux-musl`.

---

## macOS

Apple Silicon:

```bash
VERSION=v0.1.0
TARGET=aarch64-apple-darwin
ARCHIVE="numax-${VERSION}-${TARGET}.tar.gz"

curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/${ARCHIVE}"
curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/SHA256SUMS"

grep " ${ARCHIVE}$" SHA256SUMS | shasum -a 256 -c -
tar -xzf "${ARCHIVE}"
sudo install -m 755 "numax-${VERSION}-${TARGET}/nx" /usr/local/bin/nx

nx --version
```

Mac Intel:

```bash
VERSION=v0.1.0
TARGET=x86_64-apple-darwin
ARCHIVE="numax-${VERSION}-${TARGET}.tar.gz"

curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/${ARCHIVE}"
curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/SHA256SUMS"

grep " ${ARCHIVE}$" SHA256SUMS | shasum -a 256 -c -
tar -xzf "${ARCHIVE}"
sudo install -m 755 "numax-${VERSION}-${TARGET}/nx" /usr/local/bin/nx

nx --version
```

---

## Windows

Apri PowerShell:

```powershell
$Version = "v0.1.0"
$Target = "x86_64-pc-windows-msvc"
$Archive = "numax-$Version-$Target.zip"
$Base = "https://github.com/GianIac/numax/releases/download/$Version"
$InstallDir = "$env:USERPROFILE\.numax\bin"

Invoke-WebRequest "$Base/$Archive" -OutFile $Archive
Invoke-WebRequest "$Base/SHA256SUMS" -OutFile "SHA256SUMS"

$Expected = ((Select-String -Path "SHA256SUMS" -Pattern $Archive).Line -split "\s+")[0].ToLower()
$Actual = (Get-FileHash $Archive -Algorithm SHA256).Hash.ToLower()
if ($Actual -ne $Expected) { throw "Checksum mismatch for $Archive" }

Expand-Archive $Archive -DestinationPath . -Force
New-Item -ItemType Directory -Force $InstallDir | Out-Null
Copy-Item "numax-$Version-$Target\nx.exe" "$InstallDir\nx.exe" -Force

[Environment]::SetEnvironmentVariable(
  "Path",
  [Environment]::GetEnvironmentVariable("Path", "User") + ";$InstallDir",
  "User"
)

& "$InstallDir\nx.exe" --version
```

Dopo aver modificato `Path`, apri un nuovo terminale.

---

## Installare da sorgente

Questo è il percorso più affidabile mentre Numax è ancora giovane.

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo install --path crates/nx-cli --locked

nx --version
```

Se vuoi solo compilare il binario dentro il repository:

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo build --release -p nx-cli --locked

./target/release/nx --version
```

---

## Compilare moduli WASM

La CLI esegue moduli `.wasm`. Per compilare moduli guest in Rust, installa il target WASM:

```bash
rustup target add wasm32-unknown-unknown
```

Poi compila un esempio:

```bash
cd examples/distributed_counter
cargo build --release --target wasm32-unknown-unknown
```

Il modulo generato è:

```text
examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm
```

---

## Verificare l'installazione

Controlla la CLI:

```bash
nx --version
nx config init --output numax.toml
nx config validate --config numax.toml
```

Esegui un modulo:

```bash
nx run path/to/module.wasm --datastore-path ./nx-data
```

Abilita sync aggiungendo un indirizzo di ascolto:

```bash
nx run path/to/module.wasm \
  --listen 0.0.0.0:9000 \
  --peer 127.0.0.1:9001 \
  --datastore-path ./node-a
```

---

## Correlati

- [Quickstart: 5 minuti](/numax/it/getting-started/quickstart-5-min/) - esegui due nodi sincronizzati
- [Il tuo primo modulo](/numax/it/getting-started/your-first-module/) - compila un modulo guest
- [CLI reference](/numax/it/reference/cli/) - tutti i comandi e flag di `nx`
