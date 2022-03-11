/*!
 * The goal is to filter the documents based on the annotation ["short s", "header"]
 * take a document
 */
use std::{borrow::Cow, collections::HashSet, fmt::Display};

use crate::error::Error;
use sha2::digest::Reset;

use crate::ops::FilterTags;

struct FilterTagDoc;
impl FilterTags for FilterTagDoc {
    fn filter_tags(
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> Result<(), crate::error::Error> {
        todo!()
    }
}
enum Tag {
    Header,
    Footer,
    ShortSentences,
    Tiny,
    Adult,
    Noisy,
}
impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tags_string = match self {
            Tag::Header => "header",
            Tag::Footer => "footer",
            Tag::ShortSentences => "short_sentences",
            Tag::Tiny => "tiny",
            Tag::Adult => "adult",
            Tag::Noisy => "noisy",
        };
        write!(f, "{}", tags_string)
    }
}
impl FilterTagDoc {
    fn filter_single_documnet(
        doc: &str,
        include: HashSet<Cow<str>>,
        exclude: HashSet<Cow<str>>,
    ) -> Result<bool, Error> {
        let document: serde_json::Value = serde_json::from_str(&doc)?;
        match &document["annotation"] {
            serde_json::Value::Array(arr) => {
                let doc_tags = Self::convert_to_hashset(arr);
                Self::filter_tags(doc_tags, include, exclude);
                Ok(true)
            }
            other => Err(Error::MalformedContent(other.clone())),
        }
    }
    fn convert_to_hashset(arr: &[serde_json::Value]) -> HashSet<Cow<str>> {
        let mut hash_set: HashSet<Cow<str>> = HashSet::new();
        for items in arr {
            if let serde_json::Value::String(s) = items {
                hash_set.insert(Cow::from(s.to_string()));
            } else {
                panic!("non string annotation");
            }
        }
        hash_set
    }
    /// filter documents depending on tags,
    /// filtering rules
    /// - The rules for exclude:
    ///      -if doc_tags or exclude tages is empty -> do not do anything
    ///      -if doc_tages ∩ exclude tages is not empty -> false
    /// -The rules for including:
    ///     -if doc_tages is empty and include is not empty -> false
    ///     -if doc_tages is not empty and include is empty -> true
    ///     -if include is a subset of doc_tages -> true
    ///
    fn filter_tags(
        doc_tags: HashSet<Cow<str>>,
        include: HashSet<Cow<str>>,
        exclude: HashSet<Cow<str>>,
    ) -> bool {
        //check the intersection between doc_tags and exclude and if not empty return false
        //excluding rul

        if doc_tags.intersection(&exclude).count() != 0 {
            false
        } else if include.is_subset(&doc_tags) {
            true
        } else {
            match (doc_tags.is_empty(), include.is_empty()) {
                (true, false) => true,
                (false, true) => false,
                _ => include.is_subset(&doc_tags),
            }
        }
    }
}
#[cfg(test)]
mod test {
    use std::{borrow::Cow, collections::HashSet};

    use super::FilterTagDoc;
    #[test]
    fn filter_tags_excluded() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("nosiy"));
        include.insert(Cow::from("short_sentences"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, false);
    }
    #[test]
    fn filter_tags_included() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("short_sentences"));
        include.insert(Cow::from("short_sentences"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, true);
    }
    #[test]
    fn filter_tags_excluded_() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("tiny"));
        include.insert(Cow::from("short_sentences"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn filter_tags_no_tag() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        include.insert(Cow::from("short_sentences"));
       // exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn filter_tags_no_include() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("nosiy"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, false);
    }
}
