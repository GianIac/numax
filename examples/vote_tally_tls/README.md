# Vote Tally TLS Example

Three-node vote tally using Numax's replicated GCounter over mTLS with an
allowlist. Each run casts one vote for `vote:tally:yes`.

## Build

```bash
cd examples/vote_tally_tls
cargo build --release --target wasm32-unknown-unknown
```

## Generate a Local PKI

From `examples/vote_tally_tls`:

```bash
mkdir -p tls

cat > tls/ca.cnf <<'EOF'
[req]
distinguished_name = dn
x509_extensions = v3_ca
prompt = no

[dn]
CN = numax-local-ca

[v3_ca]
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign
EOF

openssl req -x509 -newkey rsa:4096 -nodes -days 365 \
    -keyout tls/ca-key.pem \
    -out tls/ca.pem \
    -config tls/ca.cnf

cat > tls/node.cnf <<'EOF'
[req]
distinguished_name = dn
prompt = no

[dn]
CN = localhost

[v3_req]
subjectAltName = DNS:localhost,IP:127.0.0.1
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth,clientAuth
EOF

for node in a b c; do
    openssl req -newkey rsa:2048 -nodes \
        -keyout "tls/node-${node}-key.pem" \
        -out "tls/node-${node}.csr" \
        -config tls/node.cnf

    openssl x509 -req -days 365 \
        -in "tls/node-${node}.csr" \
        -CA tls/ca.pem \
        -CAkey tls/ca-key.pem \
        -CAcreateserial \
        -extfile tls/node.cnf \
        -extensions v3_req \
        -out "tls/node-${node}.pem"
done
```

## Compute Node IDs

Numax derives the protocol NodeId from the certificate public key:
SHA-256 over SPKI, first 16 bytes as lowercase hex.

```bash
node_id() {
    openssl x509 -in "$1" -pubkey -noout \
        | openssl pkey -pubin -outform DER \
        | openssl dgst -sha256 -binary \
        | xxd -p -c 256 \
        | cut -c1-32
}

NODE_A=$(node_id tls/node-a.pem)
NODE_B=$(node_id tls/node-b.pem)
NODE_C=$(node_id tls/node-c.pem)
ALLOWLIST="${NODE_A},${NODE_B},${NODE_C}"

printf 'node-a=%s\nnode-b=%s\nnode-c=%s\n' "$NODE_A" "$NODE_B" "$NODE_C"
```

## Run Three Nodes

Open three terminals from `examples/vote_tally_tls`.

Terminal A:

```bash
nx run target/wasm32-unknown-unknown/release/vote_tally_tls.wasm \
    --listen 127.0.0.1:9100 \
    --peer 127.0.0.1:9101 \
    --peer 127.0.0.1:9102 \
    --datastore-path ./data-a \
    --tls-cert tls/node-a.pem \
    --tls-key tls/node-a-key.pem \
    --tls-ca tls/ca.pem \
    --allowed-peers "$ALLOWLIST" \
    --wait-before-run 2s \
    --settle-for 3s \
    --print-gcounter vote:tally:yes \
    -v
```

Terminal B:

```bash
nx run target/wasm32-unknown-unknown/release/vote_tally_tls.wasm \
    --listen 127.0.0.1:9101 \
    --peer 127.0.0.1:9100 \
    --peer 127.0.0.1:9102 \
    --datastore-path ./data-b \
    --tls-cert tls/node-b.pem \
    --tls-key tls/node-b-key.pem \
    --tls-ca tls/ca.pem \
    --allowed-peers "$ALLOWLIST" \
    --wait-before-run 2s \
    --settle-for 3s \
    --print-gcounter vote:tally:yes \
    -v
```

Terminal C:

```bash
nx run target/wasm32-unknown-unknown/release/vote_tally_tls.wasm \
    --listen 127.0.0.1:9102 \
    --peer 127.0.0.1:9100 \
    --peer 127.0.0.1:9101 \
    --datastore-path ./data-c \
    --tls-cert tls/node-c.pem \
    --tls-key tls/node-c-key.pem \
    --tls-ca tls/ca.pem \
    --allowed-peers "$ALLOWLIST" \
    --wait-before-run 2s \
    --settle-for 3s \
    --print-gcounter vote:tally:yes \
    -v
```

With the three commands above, each process casts one local vote, waits for
peer replication, prints the final host-side tally and exits. Once all three
nodes have exchanged their PushOps, each node should print:

```text
vote:tally:yes = 3
```

## Notes

- mTLS is enabled by `--tls-cert`, `--tls-key`, and `--tls-ca`.
- The allowlist admits only the three certificate-derived NodeIds.
- `--config ./numax.toml` can provide the implemented `[limits]` section:
  `max_peers`, `queued_ops_limit`, `max_message_size`, and
  `socket_timeout_secs`.
- The guest never writes votes through `nx_sdk::db::*`; replicated state goes
  through `nx_sdk::crdt::gcounter`.
- `--wait-before-run` gives the three TLS handshakes time to complete before
  the guest emits its vote.
- `--settle-for` gives PushOps and remote apply time to complete before exit.
- Without `--settle-for`, a sync-enabled runtime stays alive until SIGINT,
  SIGTERM or SIGHUP and flushes the store during shutdown.
