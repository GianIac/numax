use core::fmt;

pub type Result<T> = core::result::Result<T, NxError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NxError {
    /// Host returned -3 or generic failure.
    Internal,
    /// Host returned -2 and we exceeded the max retry cap.
    BufferTooSmall,
    /// Host returned -1 (generally used by db_get).
    NotFound,
    /// Any unexpected negative return code.
    UnknownCode(i32),
}

impl fmt::Display for NxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NxError::Internal => write!(f, "nx error: internal"),
            NxError::BufferTooSmall => write!(f, "nx error: buffer too small"),
            NxError::NotFound => write!(f, "nx error: not found"),
            NxError::UnknownCode(c) => write!(f, "nx error: unknown host code {c}"),
        }
    }
}

// #[cfg(feature = "std")]
// impl std::error::Error for NxError {}
