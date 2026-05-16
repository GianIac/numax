@echo off

clang ^
  --target=wasm32-wasip1 ^
  -O3 ^
  -nostdlib ^
  -Wl,--no-entry ^
  -Wl,--export=run ^
  -Wl,--allow-undefined ^
  -o guest.wasm ^
  src\guest.c

echo.
echo Build complete:
echo guest.wasm