#![cfg_attr(target_arch = "wasm32", no_std)]

pub extern crate alloc as __alloc;

pub mod db;
pub mod error;
pub mod log;

mod ffi;

pub use error::NxError;
