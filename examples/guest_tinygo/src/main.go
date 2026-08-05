package main

import (
	"unsafe"
)

// Import Numax host functions.
//go:wasmimport nx host_log_v2
func nxHostLogV2(ptr uint32, size uint32) int32

//go:wasmimport nx db_set
func nxDbSet(keyPtr uint32, keySize uint32, valPtr uint32, valSize uint32) int32

// The guest entrypoint that Numax calls.
//go:export run
func run() {
	log("Hello from TinyGo guest!")
	
	dbSet("hello", "numax-tinygo")

	log("db_set ok")
}

// --- Helpers to handle WASM memory boundaries ---

func log(message string) {
	ptr, size := stringToWasmPtr(message)
	nxHostLogV2(ptr, size)
}

func dbSet(key string, value string) {
	kPtr, kSize := stringToWasmPtr(key)
	vPtr, vSize := stringToWasmPtr(value)
	nxDbSet(kPtr, kSize, vPtr, vSize)
}

// stringToWasmPtr converts a Go string to a pointer and size that can be read by the Rust host.
func stringToWasmPtr(s string) (uint32, uint32) {
	buf := []byte(s)
	if len(buf) == 0 {
		return 0, 0
	}
	ptr := &buf[0]
	return uint32(uintptr(unsafe.Pointer(ptr))), uint32(len(buf))
}

// main is required for TinyGo to compile to WASM, even if we only use exports.
func main() {}