@echo off

echo Building guest_c WASM module

clang ^
  --target=wasm32-wasip1 ^
  -O3 ^
  -nostdlib ^
  -Wl,--no-entry ^
  -Wl,--export=run ^
  -Wl,--allow-undefined ^
  -o guest.wasm ^
  src\guest.c

IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo build failed.
    echo contains compiler errors
    exit /b %ERRORLEVEL%
)

echo.
echo Build complete:
echo guest.wasm
