@echo off
setlocal
cd /d "%~dp0"

rustup target add wasm32-wasip1 >nul 2>&1

set RUSTFLAGS=-C link-arg=--export=run
cargo build --release --target wasm32-wasip1
if %errorlevel% neq 0 exit /b %errorlevel%

copy /Y "target\wasm32-wasip1\release\guest.wasm" "guest.wasm"
echo Built examples\guest_python\guest.wasm