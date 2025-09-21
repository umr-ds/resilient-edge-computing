# WASI Smoke Test

Simple WASI test binary.

## Build

```bash
rustup target add wasm32-wasip1
cargo build --target wasm32-wasip1 --release
mv target/wasm32-wasip1/release/wasi-smoke.wasm ../artifacts/
```
