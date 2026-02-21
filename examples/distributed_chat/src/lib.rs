//! Distributed Chat Example (Basic)
//!
//! Ogni esecuzione:
//! 1. Mostra tutti i messaggi esistenti
//! 2. Aggiunge un nuovo messaggio (se fornito)
//!
//! ## Uso
//!
//! ```bash
//! # Nodo A - invia messaggio
//! nx run chat.wasm --sync-prefix "chat:" --datastore-path ./data-a
//! 
//! # Per aggiungere messaggio, usa variabile env MESSAGE
//! MESSAGE="Ciao!" nx run chat.wasm --sync-prefix "chat:" --datastore-path ./data-a
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

    // Leggi messaggi esistenti
    let messages = load_messages();
    
    if messages.is_empty() {
        log("(nessun messaggio)");
    } else {
        for msg in &messages {
            log(msg);
        }
    }
    
    log("");
    log("────────────────────────────────");

    // Aggiungi nuovo messaggio (hardcoded per ora)
    // In futuro: leggere da args o stdin
    let new_msg = get_new_message();
    
    if let Some(msg) = new_msg {
        log("Invio:");
        log(&msg);
        
        let mut messages = messages;
        messages.push(msg);
        save_messages(&messages);
        
        log("✓ Messaggio salvato!");
    } else {
        log("(nessun nuovo messaggio)");
    }
}

/// Carica i messaggi dal DB
fn load_messages() -> Vec<String> {
    match db::get(CHAT_KEY) {
        Ok(Some(bytes)) => {
            // Split per separatore
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

/// Salva i messaggi nel DB
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

/// Genera un messaggio di test con timestamp simulato
fn get_new_message() -> Option<String> {
    // Per ora: messaggio incrementale basato sul contatore
    let count = load_messages().len();
    
    // Ogni 3 esecuzioni aggiungiamo un messaggio
    if count < 10 {
        Some(alloc::format!("[User] Messaggio #{}", count + 1))
    } else {
        None
    }
}