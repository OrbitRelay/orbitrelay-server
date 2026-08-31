# OrbitRelay Server

OrbitRelay Server is the open-source single-node Community distribution. It
combines the public OrbitRelay Kernel with WebSocket delivery, SQLite event
storage, a local immutable asset store, a SQLite document/canvas catalog, and
a runnable server process.

The Community distribution supports local use, LAN hosting, and a single
remote relay node. It retains the complete Document, PDF, Canvas, Query,
history replay, and realtime Event capabilities of the kernel. Its deployment
boundary is one server node with local persistence and a simple identity
integration surface.

## Runtime Modes

- Memory mode for tests and temporary sessions
- Persistent EventStore through SQLite
- Persistent Asset metadata plus immutable local blobs
- Persistent Document/Page/Canvas/Layer catalog through SQLite
- Development identity and authorization only when explicitly enabled

Production startup is fail-closed when identity and authorization dependencies
are not configured.

## Development Run

```powershell
$env:ORBITRELAY_DEVELOPMENT_MODE = 'true'
$env:ORBITRELAY_BIND_ADDR = '127.0.0.1:8080'
cargo run -p orbitrelay-server
```

The default WebSocket endpoint is `ws://127.0.0.1:8080/ws`. Persistent paths
are selected with `ORBITRELAY_EVENT_STORE_PATH`,
`ORBITRELAY_ASSET_STORE_DIR`, and `ORBITRELAY_CATALOG_STORE_PATH`.

## Verification

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

This repository depends on released crates from
[`orbitrelay-kernel`](https://github.com/orbitrelay/orbitrelay-kernel). It does
not contain or depend on enterprise, cloud, billing, or product code.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
