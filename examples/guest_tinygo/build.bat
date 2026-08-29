@echo off

echo Building guest_tinygo WASM module

tinygo build -opt=0 -o guest.wasm -target wasi -no-debug src\main.go

IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo build failed.
    echo contains compiler errors
    exit /b %ERRORLEVEL%
)

echo.
echo Build complete:
echo guest.wasm
