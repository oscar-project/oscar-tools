use std::{borrow::Cow, collections::HashSet, path::Path};

use crate::error::Error;
pub trait FilterTags {
    fn filter_tags(
        src: &Path,
        dst: &Path,
        clean: bool,
        include: &HashSet<Cow<str>>,
        exclude: &HashSet<Cow<str>>,
    ) -> Result<(), Error>;
}
