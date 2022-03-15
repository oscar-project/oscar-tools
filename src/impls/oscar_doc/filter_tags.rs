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

        // Exclusion checking
        // if (doc_tags is not empty) AND
        //         (exclude is not empty)  AND
        //         intersection between doc_tags exclude is not empty
        // then it means that document has tags that should be excluded: we discard
        if !doc_tags.is_empty()
            && !exclude.is_empty()
            && doc_tags.intersection(&exclude).count() != 0
        {
            false
        } else {
            match (doc_tags.is_empty(), include.is_empty()) {
                // no annotations on doc, but we filter on a specific set of annotations, so false.
                (true, false) => false,
                // annotations on doc,  but we want NO annotations (hence include is empty), so false.
                (false, true) => false,

                // no annotations on doc, and we want no annotations, so it's true.
                (true, true) => true,

                // we got annotations and we want to filter on annotations.
                // We check that doc tags contains required tags (include).
                (false, false) => include.is_subset(&doc_tags),
            }
        }
    }
}
#[cfg(test)]
mod test {
    use std::{borrow::Cow, collections::HashSet};

    use super::FilterTagDoc;

    /*
    Cases:
    D = document tags, I= include tags, E= exlcude tags

    1. No D, no I, no E => keep (clean corpus)
    2. No D, no I, E    => keep
    3. No D, I, no E    => discard
    4. D, no I, no E    => discard

    */

    #[test]
    fn test_edge_case_1() {
        let doc_tags = HashSet::new();
        let include = HashSet::new();
        let exclude = HashSet::new();

        let res = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(res, true);
    }

    #[test]
    fn test_edge_case_2() {
        let doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        exclude.insert(Cow::from("A"));
        let res = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(res, true);
    }

    #[test]
    fn test_edge_case_3() {
        let doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();
        include.insert(Cow::from("A"));

        let res = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(res, false);
    }

    #[test]
    fn test_edge_case_4() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let exclude = HashSet::new();
        doc_tags.insert(Cow::from("A"));

        let res = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(res, false);
    }

    #[test]
    fn filter_tags_include() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();
        include.insert(Cow::from("A"));
        doc_tags.insert(Cow::from("A"));
        doc_tags.insert(Cow::from("B"));

        let res = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(res, true);
    }
    #[test]
    fn filter_tags_exclude() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        doc_tags.insert(Cow::from("A"));
        exclude.insert(Cow::from("A"));

        let res = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(res, false)
    }

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

    #[test]
    fn filter_tags_same_exclude_include() {
        // let's imagine that a tag is simultaneously included and excluded.
        // since we process exclude first, it should discard the document.
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("tiny"));
        exclude.insert(Cow::from("tiny"));
        include.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn filter_tags_complex() {
        // complex passing example with numerous doc, incl and excl tags
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("tiny"));
        doc_tags.insert(Cow::from("short_sentences"));
        doc_tags.insert(Cow::from("adult"));

        exclude.insert(Cow::from("header"));
        exclude.insert(Cow::from("noisy"));

        include.insert(Cow::from("tiny"));
        include.insert(Cow::from("adult"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, true);
    }

    #[test]
    fn filter_tags_complex_filtered() {
        // complex passing example with numerous doc, incl and excl tags
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("tiny"));
        doc_tags.insert(Cow::from("short_sentences"));
        doc_tags.insert(Cow::from("adult"));

        exclude.insert(Cow::from("header"));
        exclude.insert(Cow::from("tiny"));

        include.insert(Cow::from("noisy"));
        include.insert(Cow::from("adult"));

        let filters = FilterTagDoc::filter_tags(doc_tags, include, exclude);
        assert_eq!(filters, false);
    }
}
