//! Errors
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Json(serde_json::Error),
    ThreadPoolBuild(rayon::ThreadPoolBuildError),
    Custom(String),
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
