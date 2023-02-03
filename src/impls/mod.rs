/*! Operation implementations

This module contains implementations for operations ([crate::ops]) on OSCAR Schema specifications.

!*/
mod v1;
mod v2;
mod v3;

pub(crate) use v1::OscarTxt;
pub(crate) use v2::OscarDoc;
