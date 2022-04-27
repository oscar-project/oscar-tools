//! Errors
#[derive(Debug)]
#[cfg(not(tarpaulin_include))]
pub enum Error {
    Io(std::io::Error),
    Json(serde_json::Error),
    ThreadPoolBuild(rayon::ThreadPoolBuildError),
    MissingContent(serde_json::Value),
    MalformedContent(serde_json::Value),
    OscarIo(oscar_io::error::Error),
    Custom(String),
}

#[cfg(not(tarpaulin_include))]
impl From<oscar_io::error::Error> for Error {
    fn from(v: oscar_io::error::Error) -> Self {
        Self::OscarIo(v)
    }
}

impl From<rayon::ThreadPoolBuildError> for Error {
    fn from(v: rayon::ThreadPoolBuildError) -> Self {
        Self::ThreadPoolBuild(v)
    }
}

impl From<serde_json::Error> for Error {
    fn from(v: serde_json::Error) -> Self {
        Self::Json(v)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}
