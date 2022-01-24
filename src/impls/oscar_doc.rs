use crate::{
    ops::Split,
    versions::{Schema, Version},
};

pub struct OscarDoc;

impl Schema for OscarDoc {
    fn version() -> Version {
        Version::new(2, 0, 0)
    }
}

impl Split for OscarDoc {}
