use std::path::Path;

use crate::error::Error;
pub trait FilterTags {
    fn filter_tags(src: &Path, dst: &Path) -> Result<(), Error>;
}
