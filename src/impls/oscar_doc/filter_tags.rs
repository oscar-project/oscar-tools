/*! The goal is to filter the documents based on the annotation ["short s", "header"]
 * take a document
 */
use std::{
    borrow::Cow,
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter},
};

use crate::error::Error;

use crate::ops::FilterTags;

pub struct FilterTagDoc;
impl FilterTags for FilterTagDoc {
    fn filter_tags(
        src: &std::path::Path,
        dst: &std::path::Path,
        clean: bool,
        include: &HashSet<Cow<str>>,
        exclude: &HashSet<Cow<str>>,
    ) -> Result<(), crate::error::Error> {
        let file = File::open(src)?;
        let bufread = BufReader::new(file);
        if dst.exists() {
            error!("File exist!");
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File exist {:?}", dst),
            )
            .into());
        }
        let dst_file = File::create(dst)?;
        let mut dst_buf = BufWriter::new(dst_file);

        Self::filter_write(bufread, &mut dst_buf, clean, include, exclude)?;
        Ok(())
    }
}

impl FilterTagDoc {
    fn filter_single_document(
        doc: &str,
        clean: bool,
        include: &HashSet<Cow<str>>,
        exclude: &HashSet<Cow<str>>,
    ) -> Result<bool, Error> {
        let document: serde_json::Value = serde_json::from_str(doc)?;

        match &document["metadata"]["annotation"] {
            serde_json::Value::Array(arr) => {
                let doc_tags = Self::convert_to_hashset(arr);
                Ok(Self::apply_filter_rules(&doc_tags, include, exclude))
            }

            // If we don't have annotations
            // If we in the --clean case, we return true
            // Else if include is empty, return true
            // if include is not empty, return false
            serde_json::Value::Null => {
                if clean | include.is_empty() {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            other => {
                error!("Record has a malformed annotation field");
                debug!("{other:#?}");
                Err(Error::MalformedContent(other.clone()))
            }
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
    fn apply_filter_rules(
        doc_tags: &HashSet<Cow<str>>,
        include: &HashSet<Cow<str>>,
        exclude: &HashSet<Cow<str>>,
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
            && doc_tags.intersection(exclude).count() != 0
        {
            false
        } else {
            match (doc_tags.is_empty(), include.is_empty()) {
                // no annotations on doc, but we filter on a specific set of annotations, so false.
                (true, false) => false,
                // annotations on doc, but no include constraint.
                // Check intersection with exclude, if none, then true
                (false, true) => {
                    if exclude.is_empty() {
                        // error!("Either use --clean or provide exclude and/or include tags");
                        false
                    } else {
                        // discard doc if exclude is intersect with doc_tags
                        exclude.is_disjoint(doc_tags)
                    }
                }

                // no annotations on doc, and we want no annotations, so it's true.
                (true, true) => true,

                // we got annotations and we want to filter on annotations.
                // We check that doc tags contains required tags (include).
                (false, false) => include.is_subset(doc_tags),
            }
        }
    }
    /// Will read documents from a Reader and output document that match predicates into a Writer.
    fn filter_write<T, U>(
        src: T,
        dst: &mut U,
        clean: bool,
        include: &HashSet<Cow<str>>,
        exclude: &HashSet<Cow<str>>,
    ) -> Result<(), Error>
    where
        T: std::io::BufRead,
        U: std::io::Write,
    {
        //check if there is an overlap between include and exclude
        if (include.intersection(exclude).count() != 0)
            && !include.is_empty()
            && !exclude.is_empty()
        {
            error!("You can not include and exclude at the same time");
            return Err(Error::Custom(
                "You can not include and exclude at the same time".to_string(),
            ));
        }

        let documents = src.lines();

        // apply filter_single_document to documents
        // for documents that have not been filtered out, write them in the dst.

        let results = documents.filter_map(|doc| -> Option<String> {
            match doc {
                // if we could properly read the document from the writer,
                // match on the filtering process
                Ok(doc) => match Self::filter_single_document(&doc, clean, include, exclude) {
                    // if the doc is well formed AND matches the constraints
                    Ok(true) => Some(doc),
                    // if the doc is well formed AND doesn't match the constraints
                    Ok(false) => None,

                    // if the doc is not well formed
                    Err(e) => {
                        error!("{:?}", e);
                        None
                    }
                },
                // IO Error (probably)
                Err(_) => {
                    error!("Error reading document");
                    None
                }
            }
        });

        for mut doc in results {
            doc.push('\n');
            let bytes_to_write = doc.len();
            if bytes_to_write != dst.write(doc.as_bytes())? {
                error!("Document has not been completely written!");
                //TODO: return error?
            }
        }
        dst.flush()?;

        Ok(())
    }
}
#[cfg(test)]
mod test {
    use std::{borrow::Cow, collections::HashSet};

    use serde_json::Value;

    use super::FilterTagDoc;

    /*
    Cases:
    D = document tags, I= include tags, E= exlcude tags

    1. No D, no I, no E => keep (clean corpus)
    2. No D, no I, E    => keep
    3. No D, I, no E    => discard
    4. D, no I, no E    => discard
    5a. D, no I, E      => E and D intersects, so discard
    5b. D, no I, E      => E and D are disjoint, so keep
    */

    #[test]
    fn test_edge_case_1() {
        let doc_tags = HashSet::new();
        let include = HashSet::new();
        let exclude = HashSet::new();

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, true);
    }

    #[test]
    fn test_edge_case_2() {
        let doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        exclude.insert(Cow::from("A"));
        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, true);
    }

    #[test]
    fn test_edge_case_3() {
        let doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();
        include.insert(Cow::from("A"));

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false);
    }

    #[test]
    fn test_edge_case_4() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let exclude = HashSet::new();
        doc_tags.insert(Cow::from("A"));

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false);
    }

    #[test]
    fn test_edge_case_5a() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        doc_tags.insert(Cow::from("A"));
        exclude.insert(Cow::from("B"));

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, true);
    }

    // oscar-tools extract-tags from.jsonl to.jsonl --clean
    // oscar-tools extract-tags from.jsonl to.jsonl --include foo --exclude bar
    #[test]
    fn test_edge_case_5b() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        doc_tags.insert(Cow::from("B"));
        exclude.insert(Cow::from("B"));

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false);
    }
    #[test]
    fn apply_filter_rules_include() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();
        include.insert(Cow::from("A"));
        doc_tags.insert(Cow::from("A"));
        doc_tags.insert(Cow::from("B"));

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, true);
    }
    #[test]
    fn apply_filter_rules_exclude() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        doc_tags.insert(Cow::from("A"));
        exclude.insert(Cow::from("A"));

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false)
    }

    #[test]
    fn apply_filter_rules_excluded() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("nosiy"));
        include.insert(Cow::from("short_sentences"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }
    #[test]
    fn apply_filter_rules_included() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("short_sentences"));
        include.insert(Cow::from("short_sentences"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, true);
    }
    #[test]
    fn apply_filter_rules_excluded_() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("tiny"));
        include.insert(Cow::from("short_sentences"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn apply_filter_rules_no_tag() {
        let doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();

        include.insert(Cow::from("short_sentences"));
        // exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn apply_filter_rules_no_include() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("nosiy"));
        exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, true);
    }

    #[test]
    fn apply_filter_rules_same_exclude_include() {
        // let's imagine that a tag is simultaneously included and excluded.
        // since we process exclude first, it should discard the document.
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("tiny"));
        exclude.insert(Cow::from("tiny"));
        include.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn apply_filter_rules_complex() {
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

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, true);
    }

    #[test]
    fn apply_filter_rules_complex_filtered() {
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

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }
    #[test]
    fn apply_filter_rules_verycomplex() {
        // complex passing example with numerous doc, incl and excl tags
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert(Cow::from("short_sentences"));

        exclude.insert(Cow::from("header"));

        include.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn test_filter_write() {
        let src = r#"{"content":"words like words", "metadata":{"annotation":["tiny"]}}
{"content":"when to use\n it", "metadata": {"annotation":null}}
{"content":"to start\n with", "metadata": {"annotation":null}}
{"content":"to start\n with", "metadata": {"annotation":["tiny", "header"]}}"#;
        let mut dst = vec![];
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();
        let including_tag = "tiny";
        let excluding_tag = "header";
        include.insert(Cow::from(including_tag));
        exclude.insert(Cow::from(excluding_tag));

        FilterTagDoc::filter_write(src.as_bytes(), &mut dst, false, &include, &exclude).unwrap();
        let dst = String::from_utf8_lossy(&dst);
        for doc in dst.lines() {
            let doc: serde_json::Value = serde_json::from_str(doc).unwrap();
            let annotation = &doc["annotation"];
            if let Value::Array(a) = annotation {
                assert!(!a
                    .iter()
                    .any(|annotation: &Value| annotation == &Value::String("header".to_string())));
                assert!(a
                    .iter()
                    .any(|annotation: &Value| annotation == &Value::String("tiny".to_string())));
            }
        }
    }

    #[test]
    fn test_filter_write_overlap() {
        // the idea is to test the case where there is an overlap between include and exclude.
        let src = r#"{"content":"words like words", "metadata":{"annotation":["tiny"]}}
{"content":"when to use\n it", "metadata": {"annotation":null}}
{"content":"to start\n with", "metadata": {"annotation":null}}
{"content":"to start\n with", "metadata": {"annotation":["tiny", "header"]}}"#;
        let mut dst = vec![];
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();
        let including_tags = ["short_sentence", "tiny"].map(|a| Cow::from(a));
        let excluding_tags = ["header", "short_sentence"].map(|a| Cow::from(a));
        include.extend(including_tags);
        exclude.extend(excluding_tags);

        assert!(
            FilterTagDoc::filter_write(src.as_bytes(), &mut dst, false, &include, &exclude)
                .is_err()
        );
    }
    #[test]
    fn test_filter_write_clean() {
        let src = r#"{"content":"words like words", "metadata":{"annotation":["tiny"]}}
{"content":"when to use\n it", "metadata": {"annotation":null}}
{"content":"to start\n with", "metadata": {"annotation":null}}
{"content":"to start\n with", "metadata": {"annotation":["tiny", "header"]}}"#;
        let mut dst = vec![];
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();
        let including_tag = "tiny";
        let excluding_tag = "header";
        include.insert(Cow::from(including_tag));
        exclude.insert(Cow::from(excluding_tag));

        FilterTagDoc::filter_write(src.as_bytes(), &mut dst, true, &include, &exclude).unwrap();
        let dst = String::from_utf8_lossy(&dst);
        for doc in dst.lines() {
            let doc: serde_json::Value = serde_json::from_str(doc).unwrap();
            let annotation = &doc["annotation"];

            if let serde_json::Value::Null = annotation {
            } else {
                panic!("{:#?}", annotation)
            }
        }
    }
}
