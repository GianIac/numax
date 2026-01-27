use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NxError {
    Internal,       // -3
    BufferTooSmall, // -2 (gestito automaticamente da db::get)
}

impl fmt::Display for NxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NxError::Internal => write!(f, "nx error: internal"),
            NxError::BufferTooSmall => write!(f, "nx error: buffer too small"),
        }
    }
}

// #[cfg(feature = "std")]
// impl std::error::Error for NxError {}
