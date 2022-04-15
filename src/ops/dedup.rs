//! Deduplication

use std::path::Path;

use crate::error::Error;

pub trait Dedup {
    fn dedup(&mut self, src: &Path, dst: &Path) -> Result<(), Error>;
}
