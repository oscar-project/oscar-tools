//! Version trait

use std::fmt::Display;
pub(crate) trait Schema {
    fn version() -> Version;
}

#[derive(PartialEq, Eq)]
pub struct Version {
    major: u32,
    minor: u32,
    patch: u32,
}

impl Version {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Version {
            major,
            minor,
            patch,
        }
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: replace by match?
        if self.minor == 0 {
            write!(f, "v{}", self.major)
        } else if self.patch == 0 {
            write!(f, "v{}.{}", self.major, self.minor)
        } else {
            write!(f, "v{}.{}.{}", self.major, self.minor, self.patch)
        }
    }
}
