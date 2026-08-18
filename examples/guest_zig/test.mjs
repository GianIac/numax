import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const moduleBytes = await readFile(new URL("guest.wasm", import.meta.url));
const module = await WebAssembly.compile(moduleBytes);

assert.deepEqual(WebAssembly.Module.imports(module), [
  { module: "nx", name: "host_log_v2", kind: "function" },
]);

const exportNames = WebAssembly.Module.exports(module).map(({ name }) => name);
assert(exportNames.includes("memory"));
assert(exportNames.includes("run"));

let memory;
let message;
const instance = await WebAssembly.instantiate(module, {
  nx: {
    host_log_v2(pointer, length) {
      const bytes = new Uint8Array(memory.buffer, pointer, length);
      message = new TextDecoder().decode(bytes);
      return 0;
    },
  },
});

memory = instance.exports.memory;
instance.exports.run();

assert.equal(message, "Hello from Zig guest!");
console.log("Zig guest ABI smoke test passed");
