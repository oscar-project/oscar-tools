//! OSCAR Schema v2 (See [oscar-corpus.com](https://oscar-corpus.com)) operation implementations.
//!
//! Implementations mostly use default trait implementations, as the format is simple.
use crate::{
    ops::Split,
    versions::{Schema, Version},
};

/// OSCAR Schema v2.
///
/// Document-oriented, one document per line, formatted in JSONLines.
pub struct OscarDoc;

impl Schema for OscarDoc {
    fn version() -> Version {
        Version::new(2, 0, 0)
    }
}

/// Use default implementation of splitting (see [crate::ops::Split])
impl Split for OscarDoc {}
