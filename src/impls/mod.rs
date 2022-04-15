/*! Operation implementations

This module contains implementations for operations ([crate::ops]) on OSCAR Schema specifications.

!*/
mod oscar_doc;
mod oscar_txt;

pub(crate) use oscar_doc::OscarDoc;
pub(crate) use oscar_txt::OscarTxt;
