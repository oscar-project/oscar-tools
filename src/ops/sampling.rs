use std::{collections::HashMap, path::Path};

use crate::error::Error;

pub trait SampleText{
    fn sample(src:&Path, dst:&Path, sample_size: usize) -> Result<(), Error>;
}