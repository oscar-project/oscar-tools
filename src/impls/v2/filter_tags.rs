/*! The goal is to filter the documents based on the annotation ["short s", "header"]
 * take a document
 */
use std::{borrow::Cow, collections::HashSet, fs::File, io::BufWriter};

use oscar_io::oscar_doc::{Document, SplitFolderReader, Writer};

use crate::error::Error;

use crate::ops::FilterTags;

pub struct FilterTagDoc;
impl FilterTags for FilterTagDoc {
    fn filter_tags(
        src: &std::path::Path,
        dst: &std::path::Path,
        clean: bool,
        include: &HashSet<&str>,
        exclude: &HashSet<&str>,
    ) -> Result<(), crate::error::Error> {
        let dst_file = File::create(dst)?;
        let dst_buf = BufWriter::new(dst_file);

        let mut cr = SplitFolderReader::new(src)?;
        let mut wr = Writer::new(dst_buf);
        Self::filter_write(&mut cr, &mut wr, clean, include, exclude)?;
        Ok(())
    }
}

impl FilterTagDoc {
    fn filter_single_document(
        doc: &Document,
        clean: bool,
        include: &HashSet<&str>,
        exclude: &HashSet<&str>,
    ) -> Result<bool, Error> {
        #[inline]
        fn check_empty_cond(clean: bool, include: &HashSet<&str>) -> Result<bool, Error> {
            if clean | include.is_empty() {
                Ok(true)
            } else {
                Ok(false)
            }
        }

        match &doc.metadata().annotation() {
            Some(annotations) => {
                if annotations.is_empty() {
                    check_empty_cond(clean, include)
                } else {
                    let doc_tags: HashSet<&str> = annotations.iter().map(|x| x.as_str()).collect();
                    Ok(Self::apply_filter_rules(&doc_tags, include, exclude))
                }
            }
            None => check_empty_cond(clean, include),
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
        doc_tags: &HashSet<&str>,
        include: &HashSet<&str>,
        exclude: &HashSet<&str>,
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
        dst: &mut Writer<U>,
        clean: bool,
        include: &HashSet<&str>,
        exclude: &HashSet<&str>,
    ) -> Result<(), Error>
    where
        T: Iterator<Item = Result<Document, oscar_io::error::Error>>,
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

        // apply filter_single_document to documents
        // for documents that have not been filtered out, write them in the dst.

        let results = src.filter_map(|doc| -> Option<Document> {
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

        for doc in results {
            dst.write(&doc)?;
        }
        dst.flush()?;

        Ok(())
    }
}
#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        io::Cursor,
        str::FromStr,
    };

    use oscar_io::{
        common::Identification,
        oscar_doc::{Document, Metadata, Reader, Writer},
    };
    use oxilangtag::LanguageTag;

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
        exclude.insert("A");
        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, true);
    }

    #[test]
    fn test_edge_case_3() {
        let doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();
        include.insert("A");

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false);
    }

    #[test]
    fn test_edge_case_4() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let exclude = HashSet::new();
        doc_tags.insert("A");

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false);
    }

    #[test]
    fn test_edge_case_5a() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        doc_tags.insert("A");
        exclude.insert("B");

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
        doc_tags.insert("B");
        exclude.insert("B");

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false);
    }
    #[test]
    fn apply_filter_rules_include() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();
        include.insert("A");
        doc_tags.insert("A");
        doc_tags.insert("B");

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, true);
    }
    #[test]
    fn apply_filter_rules_exclude() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();
        doc_tags.insert("A");
        exclude.insert("A");

        let res = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(res, false)
    }

    #[test]
    fn apply_filter_rules_excluded() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert("nosiy");
        include.insert("short_sentences");
        exclude.insert("tiny");

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }
    #[test]
    fn apply_filter_rules_included() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert("short_sentences");
        include.insert("short_sentences");
        exclude.insert("tiny");

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, true);
    }
    #[test]
    fn apply_filter_rules_excluded_() {
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert("tiny");
        include.insert("short_sentences");
        exclude.insert("tiny");

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn apply_filter_rules_no_tag() {
        let doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let exclude = HashSet::new();

        include.insert("short_sentences");
        // exclude.insert(Cow::from("tiny"));

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn apply_filter_rules_no_include() {
        let mut doc_tags = HashSet::new();
        let include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert("nosiy");
        exclude.insert("tiny");

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

        doc_tags.insert("tiny");
        exclude.insert("tiny");
        include.insert("tiny");

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn apply_filter_rules_complex() {
        // complex passing example with numerous doc, incl and excl tags
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert("tiny");
        doc_tags.insert("short_sentences");
        doc_tags.insert("adult");

        exclude.insert("header");
        exclude.insert("noisy");

        include.insert("tiny");
        include.insert("adult");

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, true);
    }

    #[test]
    fn apply_filter_rules_complex_filtered() {
        // complex passing example with numerous doc, incl and excl tags
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert("tiny");
        doc_tags.insert("short_sentences");
        doc_tags.insert("adult");

        exclude.insert("header");
        exclude.insert("tiny");

        include.insert("noisy");
        include.insert("adult");

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }
    #[test]
    fn apply_filter_rules_verycomplex() {
        // complex passing example with numerous doc, incl and excl tags
        let mut doc_tags = HashSet::new();
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();

        doc_tags.insert("short_sentences");

        exclude.insert("header");

        include.insert("tiny");

        let filters = FilterTagDoc::apply_filter_rules(&doc_tags, &include, &exclude);
        assert_eq!(filters, false);
    }

    #[test]
    fn test_filter_write() {
        let contents = [
            "words like words",
            "when to use\n it",
            "to start\n with",
            "to start\n with",
        ];
        let lang_tag = LanguageTag::parse("en").expect("unable to parse language tag");
        let metadata = [Some(vec!["tiny"]), None, None, Some(vec!["tiny", "header"])];
        let documents: Vec<_> = contents
            .into_iter()
            .zip(metadata.into_iter())
            .map(|(content, metadata)| {
                let metadata: Option<Vec<String>> =
                    metadata.map(|a| a.into_iter().map(String::from).collect());
                Ok(Document::new(
                    content.to_string(),
                    HashMap::new(),
                    Metadata::new(&Identification::new(lang_tag.into(), 1.0), &metadata, &[]),
                ))
            })
            .collect();

        //         let src = r#"{"content":"words like words", "metadata":{"annotation":["tiny"]}}
        // {"content":"when to use\n it", "metadata": {"annotation":null}}
        // {"content":"to start\n with", "metadata": {"annotation":null}}
        // {"content":"to start\n with", "metadata": {"annotation":["tiny", "header"]}}"#;
        let mut dst = vec![];
        let mut wr = Writer::new(&mut dst);
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();
        let including_tag = "tiny";
        let excluding_tag = "header";
        include.insert(including_tag);
        exclude.insert(excluding_tag);

        FilterTagDoc::filter_write(documents.into_iter(), &mut wr, false, &include, &exclude)
            .unwrap();

        let dst_reader = Cursor::new(dst);
        let cr = Reader::new(dst_reader);
        for doc in cr {
            let doc = doc.unwrap();
            let annotations = doc.metadata().annotation();
            if let Some(annotations) = annotations {
                assert!(annotations.contains(&"tiny".to_string()));
                assert!(!annotations.contains(&"header".to_string()));
            }
        }
    }
    #[test]
    fn test_filter_write_overlap() {
        // the idea is to test the case where there is an overlap between include and exclude.
        let contents = [
            "words like words",
            "when to use\n it",
            "to start\n with",
            "to start\n with",
        ];
        let lang_tag = LanguageTag::parse("en").expect("unable to parse language tag");
        let metadata = [Some(vec!["tiny"]), None, None, Some(vec!["tiny", "header"])];
        let documents: Vec<_> = contents
            .into_iter()
            .zip(metadata.into_iter())
            .map(|(content, metadata)| {
                let metadata: Option<Vec<String>> =
                    metadata.map(|a| a.into_iter().map(String::from).collect());
                Ok(Document::new(
                    content.to_string(),
                    HashMap::new(),
                    Metadata::new(&Identification::new(lang_tag.into(), 1.0), &metadata, &[]),
                ))
            })
            .collect();
        let mut dst = vec![];
        let mut wr = Writer::new(&mut dst);
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();
        let including_tags = ["short_sentence", "tiny"];
        let excluding_tags = ["header", "short_sentence"];
        include.extend(including_tags);
        exclude.extend(excluding_tags);

        assert!(FilterTagDoc::filter_write(
            documents.into_iter(),
            &mut wr,
            false,
            &include,
            &exclude
        )
        .is_err());
    }

    #[test]
    fn test_filter_write_clean() {
        let contents = [
            "words like words",
            "when to use\n it",
            "to start\n with",
            "to start\n with",
        ];
        let lang_tag = LanguageTag::parse("en").expect("unable to parse language tag");
        let metadata = [Some(vec!["tiny"]), None, None, Some(vec!["tiny", "header"])];
        let documents: Vec<_> = contents
            .into_iter()
            .zip(metadata.into_iter())
            .map(|(content, metadata)| {
                let metadata: Option<Vec<String>> =
                    metadata.map(|a| a.into_iter().map(String::from).collect());
                Ok(Document::new(
                    content.to_string(),
                    HashMap::new(),
                    Metadata::new(&Identification::new(lang_tag.into(), 1.0), &metadata, &[]),
                ))
            })
            .collect();
        let mut dst = vec![];
        let mut wr = Writer::new(&mut dst);
        let mut include = HashSet::new();
        let mut exclude = HashSet::new();
        let including_tag = "tiny";
        let excluding_tag = "header";
        include.insert(including_tag);
        exclude.insert(excluding_tag);

        FilterTagDoc::filter_write(documents.into_iter(), &mut wr, true, &include, &exclude)
            .unwrap();
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
