@external("nx", "host_log_v2")
declare function hostLogV2(message: usize, length: i32): i32;

const MESSAGE = memory.data<u8>([
  72, 101, 108, 108, 111, 32, 102, 114, 111, 109, 32, 65, 115, 115, 101, 109, 98, 108, 121, 83,
  99, 114, 105, 112, 116, 33,
]);
const MESSAGE_LENGTH: i32 = 26;

export function run(): void {
  hostLogV2(MESSAGE, MESSAGE_LENGTH);
}
