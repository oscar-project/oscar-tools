/*! Extracts textual content into new files, discarding metadata.
!*/

use std::path::Path;

use crate::error::Error;
pub trait ExtractText {
    fn extract_text(src: &Path, dst: &Path, del_src: bool) -> Result<(), Error>;
}
