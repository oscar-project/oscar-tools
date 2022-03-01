/*! Extracts textual content into new files, discarding metadata.
!*/

use std::path::Path;

use crate::error::Error;
pub trait ExtractText {
    fn extract_text(src: &Path, dst: &Path, del_src: bool) -> Result<(), Error> {
        todo!()
    }
    fn extact_json(doc: &str) -> Result<String, Error> {
        let document: serde_json::Value = serde_json::from_str(doc)?;
        match &document["content"] {
            serde_json::Value::String(content) => Ok(content.to_string()),
            other => Err(Error::MalformedContent(other.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;

    use super::ExtractText;
    struct TestExtract;
    impl ExtractText for TestExtract {}
    #[test]
    fn test_extract_json() {
        let test = r#"{"content":"foo"}"#;
        let res = TestExtract::extact_json(test);
        assert_eq!("foo", res.unwrap());
    }
    #[test]
    fn test_not_string() {
        let test = r#"{"content":22}"#;
        let res = TestExtract::extact_json(test);
        assert!(res.is_err());
        match res.unwrap_err() {
            Error::MalformedContent(_) => assert!(true),
            _ => assert!(false),
        }
    }
}
