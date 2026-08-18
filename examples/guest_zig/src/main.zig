const message = "Hello from Zig guest!";

extern "nx" fn host_log_v2(pointer: [*]const u8, length: usize) i32;

export fn run() void {
    _ = host_log_v2(message.ptr, message.len);
}
