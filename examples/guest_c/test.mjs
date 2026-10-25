import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

// Load and compile the generated WASM module
const moduleBytes = await readFile(new URL("guest.wasm", import.meta.url));
const module = await WebAssembly.compile(moduleBytes);

// inspection of the imports and exports.
// console.log("Imports:");
// console.log(WebAssembly.Module.imports(module));

// console.log("Exports:");
// console.log(WebAssembly.Module.exports(module));

// Verify that the guest imports the expected Numax host functions
assert.deepEqual(WebAssembly.Module.imports(module), [
  { module: "nx", name: "host_log_v2", kind: "function" },
  { module: "nx", name: "db_set", kind: "function" },
]);

// The expected guest entrypoint is exported
const exportNames = WebAssembly.Module.exports(module).map(({ name }) => name);
assert(exportNames.includes("run"));

let memory;
const messages = [];
const dbEntries = [];

// Instantiate the guest with mock implementations of the Numax host functions
const instance = await WebAssembly.instantiate(module, {
  nx: {
    host_log_v2(pointer, length) {
      const bytes = new Uint8Array(memory.buffer, pointer, length);
      messages.push(new TextDecoder().decode(bytes));
      return 0;
    },

    db_set(keyPointer, keyLength, valuePointer, valueLength) {
      const decoder = new TextDecoder();

      const key = decoder.decode(
        new Uint8Array(memory.buffer, keyPointer, keyLength),
      );

      const value = decoder.decode(
        new Uint8Array(memory.buffer, valuePointer, valueLength),
      );

      dbEntries.push({ key, value });
      return 0;
    },
  },
});

// Capture and verify the guest's exported memory
memory = instance.exports.memory;
assert(memory instanceof WebAssembly.Memory);

// Execute the guest entrypoint.
instance.exports.run();

// Verify the messages sent through the host logging function
assert.deepEqual(messages, [
  "Hello from C guest!",
  "db_set ok",
]);

// Verify the key/value pair passed to the host datastore function
assert.deepEqual(dbEntries, [
  { key: "hello", value: "numax" },
]);

console.log("C guest ABI smoke test passed");