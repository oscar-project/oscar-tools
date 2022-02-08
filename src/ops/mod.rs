//! Operation traits
//!
//! A subset of these should be implemented for different corpus versions.
mod compress;
mod split;
pub(crate) use compress::Compress;
pub(crate) use split::Split;
