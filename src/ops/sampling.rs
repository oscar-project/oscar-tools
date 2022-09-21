use std::path::Path;

use crate::error::Error;

pub enum SamplingKind {
    WithReplacement,
    WithoutReplacement,
}
pub trait SampleText {
    fn sample(
        src: &Path,
        dst: &Path,
        sample_size: usize,
        sampling: SamplingKind,
    ) -> Result<(), Error>;
}
