import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const wasm = await readFile(new URL("./guest.wasm", import.meta.url));
const module = await WebAssembly.compile(wasm);

assert.deepEqual(WebAssembly.Module.imports(module), [
  { module: "nx", name: "host_log_v2", kind: "function" },
]);
assert.ok(
  WebAssembly.Module.exports(module).some(
    ({ name, kind }) => name === "memory" && kind === "memory",
  ),
);
assert.ok(
  WebAssembly.Module.exports(module).some(
    ({ name, kind }) => name === "run" && kind === "function",
  ),
);

let memory;
const messages = [];
const instance = await WebAssembly.instantiate(module, {
  nx: {
    host_log_v2(pointer, length) {
      messages.push(
        new TextDecoder().decode(new Uint8Array(memory.buffer, pointer, length)),
      );
      return 0;
    },
  },
});
memory = instance.exports.memory;
instance.exports.run();

assert.deepEqual(messages, ["Hello from AssemblyScript!"]);
