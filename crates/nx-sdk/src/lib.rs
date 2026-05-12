#![cfg_attr(target_arch = "wasm32", no_std)]

pub extern crate alloc as __alloc;

pub mod crdt;
pub mod db;
pub mod error;
pub mod log;

pub use crate::log::log;
pub use error::{NxError, Result};

mod ffi;
