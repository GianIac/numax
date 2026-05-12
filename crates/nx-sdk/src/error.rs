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
    /// Host returned -4: the key lives under the runtime-reserved prefix
    /// (`__nx/...`) and cannot be accessed from guest code.
    ReservedKey,
    /// Host returned -5: the called API requires sync to be enabled on the runtime (`--listen`), but it is disabled.
    SyncDisabled,
    /// Any unexpected negative return code.
    UnknownCode(i32),
}

impl fmt::Display for NxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NxError::Internal => write!(f, "nx error: internal"),
            NxError::BufferTooSmall => write!(f, "nx error: buffer too small"),
            NxError::NotFound => write!(f, "nx error: not found"),
            NxError::ReservedKey => write!(f, "nx error: reserved key (__nx/* is runtime-only)"),
            NxError::SyncDisabled => write!(f, "nx error: sync disabled on this runtime"),
            NxError::UnknownCode(c) => write!(f, "nx error: unknown host code {c}"),
        }
    }
}

// #[cfg(feature = "std")]
// impl std::error::Error for NxError {}
