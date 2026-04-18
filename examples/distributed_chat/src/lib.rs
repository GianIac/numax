//! Distributed Chat Example (Basic)
//!
//! Each execution:
//! 1. Displays all existing messages
//! 2. Adds a new message (if provided)
//!
//! ## Usage
//!
//! ```bash
//! # Node A - send message
//! nx run chat.wasm --sync-prefix "chat:" --datastore-path ./data-a
//!
//! # To add a message, use the MESSAGE env variable
//! MESSAGE="Hello!" nx run chat.wasm --sync-prefix "chat:" --datastore-path ./data-a
//! ```

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use nx_sdk::{db, log};

const CHAT_KEY: &str = "chat:messages";
const MSG_SEPARATOR: u8 = 0x1E; // Record separator

#[no_mangle]
pub extern "C" fn run() {
    log("╔══════════════════════════════╗");
    log("║     NUMAX DISTRIBUTED CHAT   ║");
    log("╚══════════════════════════════╝");
    log("");

    // Read existing messages
    let messages = load_messages();
    
    if messages.is_empty() {
        log("(no messages)");
    } else {
        for msg in &messages {
            log(msg);
        }
    }
    
    log("");
    log("────────────────────────────────");

    // Add new message (hardcoded for now)
    // In the future: read from args or stdin
    let new_msg = get_new_message();
    
    if let Some(msg) = new_msg {
        log("Sending:");
        log(&msg);
        
        let mut messages = messages;
        messages.push(msg);
        save_messages(&messages);
        
        log("✓ Message saved!");
    } else {
        log("(no new message)");
    }
}

/// Loads messages from the DB.
fn load_messages() -> Vec<String> {
    match db::get(CHAT_KEY) {
        Ok(Some(bytes)) => {
            // Split by separator
            bytes
                .split(|&b| b == MSG_SEPARATOR)
                .filter(|s| !s.is_empty())
                .filter_map(|b| core::str::from_utf8(b).ok())
                .map(|s| s.to_string())
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Saves messages to the DB.
fn save_messages(messages: &[String]) {
    let mut bytes = Vec::new();
    
    for (i, msg) in messages.iter().enumerate() {
        if i > 0 {
            bytes.push(MSG_SEPARATOR);
        }
        bytes.extend_from_slice(msg.as_bytes());
    }
    
    let _ = db::set(CHAT_KEY, &bytes);
}

/// Generates a test message with a simulated timestamp.
fn get_new_message() -> Option<String> {
    // For now: incremental message based on the counter
    let count = load_messages().len();
    
    // Every 3 executions we add a message
    if count < 10 {
        Some(alloc::format!("[User] Message #{}", count + 1))
    } else {
        None
    }
}