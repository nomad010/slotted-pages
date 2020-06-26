//! Contains Error types and Results that are used in the project.

use crate::EntryID;

/// Represents the errors that are possible for the library.
#[derive(thiserror::Error, Debug)]
pub enum SlottedPageError {
    /// An error that occurs when loading a file and the header could not be properly loaded.
    #[error("Malformed file header")]
    MalformedFileHeader,

    /// An error when trying to load a page not known by the library or is corrupt.
    #[error("Unknown page type")]
    UnknownPageType(u8),

    /// This is returned when the underlying storage mechanism fails.
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// An error for trying to load an nonexistant entry.
    #[error("Entry `{}` not found on page `{}`", .0.slot, .0.page)]
    NotFound(EntryID),

    /// More memory was requested than a guard had available.
    #[error("Insufficient space for request. `{0}` requested, but `{1}` available")]
    InsufficientSpace(usize, usize),

    /// An error occurred during serialization.
    #[error(transparent)]
    SerializationError(anyhow::Error),

    /// An error occurred during deserialization.
    #[error(transparent)]
    DeserializationError(anyhow::Error),
}

/// The result type used to indicate an error in the library.
pub type Result<T> = std::result::Result<T, SlottedPageError>;
