//! Operation traits and basic implementations for some of them.
//!
//! A subset of these should be implemented for different corpus versions.
mod checksum;
mod compress;
mod dedup;
mod extract_text;
mod filter_tags;
mod split;
mod sampling;

pub(crate) use checksum::Checksum;
pub(crate) use compress::Compress;
pub(crate) use dedup::Dedup;
pub(crate) use extract_text::ExtractText;
pub(crate) use filter_tags::FilterTags;
pub(crate) use split::Split;
pub(crate) use sampling::SampleText;

