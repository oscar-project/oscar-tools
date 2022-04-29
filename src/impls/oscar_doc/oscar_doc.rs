//! OSCAR Schema v2 (See [oscar-corpus.com](https://oscar-corpus.com)) operation implementations.
//!
//! Implementations mostly use default trait implementations, as the format is simple.
use crate::ops::FilterTags;
use crate::{
    cli::Command,
    error::Error,
    ops::{Checksum, Compress, ExtractText, Split},
    versions::{Schema, Version},
};
use clap::{arg, ArgMatches};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashSet;
use std::{
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
};

use super::filter_tags::FilterTagDoc;

/// OSCAR Schema v2.
///
/// Document-oriented, one document per line, formatted in JSONLines.
//#[derive(clap::StructOpt)]
pub struct OscarDoc;

impl Command for OscarDoc {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        // add commands here
        let subcommand = clap::App::new(Self::version().to_string())
            .subcommand(SplitDoc::subcommand())
            .subcommand(CompressDoc::subcommand())
            .subcommand(ChecksumDoc::subcommand())
            .subcommand(ExtractFromDoc::subcommand())
            .subcommand(FilterTagDoc::subcommand());

        subcommand
    }

    fn run(matches: &ArgMatches) -> Result<(), Error> {
        let (subcommand, matches) = matches.subcommand().unwrap();
        debug!("subcommand is {subcommand}");
        match subcommand {
            "split" => SplitDoc::run(matches),
            "compress" => CompressDoc::run(matches),
            "checksum" => ChecksumDoc::run(matches),
            "extract-text" => ExtractFromDoc::run(matches),
            "extract-tags" => FilterTagDoc::run(matches),
            x => Err(Error::Custom(format!(
                "{x} op is not supported on this corpus version"
            ))),
        }
    }
}

impl Command for FilterTagDoc {
    fn run(matches: &ArgMatches) -> Result<(), Error>
    where
        Self: Sized,
    {
        debug!("Got params {:#?}", matches);
        let src: PathBuf = matches
            .value_of("SOURCE")
            .expect("Value of 'SOURCE' is required.")
            .into();
        let dst: PathBuf = matches
            .value_of("DESTINATION")
            .unwrap()
            //.expect("Value of 'DESTINATION' is required.")
            .into();
        let include: HashSet<String> = match matches.values_of("include") {
            Some(m) => m.map(|x| String::from(x)).collect(),
            None => HashSet::new(),
        };
        let exclude: HashSet<String> = match matches.values_of("exclude") {
            Some(m) => m.map(|x| String::from(x)).collect(),
            None => HashSet::new(),
        };

        let include = include.iter().map(|x| x.as_str()).collect();
        let exclude = exclude.iter().map(|x| x.as_str()).collect();
        let clean = matches.is_present("clean");
        debug!("extracting from {:?} to {:?}", src, dst);
        debug!("Including {:?}", include);
        debug!("Excluding {:?}", exclude);
        Self::filter_tags(&src, &dst, clean, &include, &exclude)
            .expect("Error while filtering documents based on tags");
        Ok(())
    }

    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        clap::App::new("extract-tags")
            .about("Extracts a OSCAR v2 corpus restricting tags. Included tags must be present and excluded ones must be absent. Use --clean to extract documents with no annotation only")
            .arg(arg!(--include <tags> "space separated tags to include.").required(false).min_values(1).short('i'))
                .arg(arg!(--exclude <tags> "space separated tags to exclude.").required(false).min_values(1).short('e'))
                .arg(arg!(--clean  "only return documents with no tags. include and exclude will be ignored").required(false))
                .arg(arg!([SOURCE] "Corpus source file/folder. If folder, splits corpus files in provided folder"))
                .arg(arg!([DESTINATION] "Corpus source file/folder. If folder, splits corpus files in provided folder"))
    }
}
impl Schema for OscarDoc {
    fn version() -> Version {
        Version::new(2, 0, 0)
    }
}
struct ExtractFromDoc;
impl ExtractText for ExtractFromDoc {}
impl Command for ExtractFromDoc {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        clap::App::new("extract-text")
            .about("Extract text from documents. The output will be a OSCAR v1 (2019)-compatible corpus.")
            .arg(arg!([SOURCE] "Corpus source file.").required(true))
            .arg(arg!([DESTINATION] "Corpus destination file (OSCAR v1 (2019)-like)").required(true))
            .arg(
                arg!(--del_src "If set, deletes source files as they are being extracted.")
                    .required(false),
            )
    }

    fn run(matches: &ArgMatches) -> Result<(), Error>
    where
        Self: Sized,
    {
        let src: PathBuf = matches.value_of("SOURCE").unwrap().into();
        let dst: PathBuf = matches.value_of("DESTINATION").unwrap().into();
        let del_src = matches.is_present("del_src");
        Self::extract_from_path(&src, &dst, del_src)
    }
}
struct ChecksumDoc;
impl Checksum for ChecksumDoc {}
impl Command for ChecksumDoc {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        clap::App::new("checksum")
        .about("Generate a checksum file for each subfolder of the provided path.")
            .arg(arg!([SOURCE] "Corpus source folder."))
            .arg(arg!(-J --num_threads <NUM_THREADS> "Number of threads to use. If 0, take all available").default_value("0").required(false))
    }

    fn run(matches: &ArgMatches) -> Result<(), Error>
    where
        Self: Sized,
    {
        let src: PathBuf = matches
            .value_of("SOURCE")
            .expect("Value of 'SOURCE' is required.")
            .into();
        let num_threads: usize = matches
            .value_of("num_threads")
            .unwrap()
            .parse()
            .expect("'num_threads' has to be a number.");

        ChecksumDoc::checksum_folder(&src, num_threads)?;
        Ok(())
    }
}
/// internal struct for split implementation
struct SplitDoc;
/// Use default implementation of splitting (see [crate::ops::Split])
impl Split for SplitDoc {}
impl Command for SplitDoc {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        clap::App::new("split")
        .about("File splitting. Supports file and folder splitting.")
        .long_about("if SOURCE is a file, DESTINATION must be a valid file path.
if SOURCE is a folder, DESTINATION must be an empty folder. Subfolders will be created for each file in SOURCE folder.")
            .arg(arg!([SOURCE] "Corpus source file/folder. If folder, splits corpus files in provided folder"))
            .arg(arg!([DESTINATION] "File/folder to write to."))
            .arg(arg!(-s --size <SIZE_MB> "Split size (in MBytes)").default_value("500").required(false))
            .arg(arg!(-J --num_threads <NUM_THREADS> "Number of threads to use (iif source is a folder). If 0, take all available").default_value("0").required(false))
    }

    fn run(matches: &ArgMatches) -> Result<(), Error>
    where
        Self: Sized,
    {
        debug!("running splitting");
        let src: PathBuf = matches
            .value_of("SOURCE")
            .expect("Value of 'SOURCE' is required.")
            .into();
        let dst: PathBuf = matches
            .value_of("DESTINATION")
            .expect("Value of 'DESTINATION' is required.")
            .into();

        // parse size and convert from MBytes into Bytes
        let size: usize = matches
            .value_of("size")
            .unwrap()
            .parse::<usize>()
            .expect("'size' has to be a number.")
            * 1_000_000usize;
        let num_threads: usize = matches
            .value_of("num_threads")
            .unwrap()
            .parse()
            .expect("'num_threads' has to be a number.");

        if src.is_file() {
            SplitDoc::split_file(&src, &dst, size)?;
        } else if src.is_dir() {
            SplitDoc::split_all(&src, &dst, size, num_threads)?;
        } else {
            return Err(
                std::io::Error::new(std::io::ErrorKind::NotFound, format!("{:?}", src)).into(),
            );
        }

        Ok(())
    }
}

/// internal struct for compression op implementation
struct CompressDoc;
impl Compress for CompressDoc {}
impl Command for CompressDoc {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        clap::App::new("compress")
        .about("Compress provided file and/or files in provided folder, up to a depth of 2.")
        .long_about("Compression of corpus files and folders.

This command can be used to compress a single file (by specifying a source and destination file path) or a set of files (by specifying a source and destination folder path).

If a file path is specified, oscar-tools will compress the given file and write it in the destination file path.
If a folder is specified, oscar-tools will compress files in subfolders and write the compressed files in the destination folder path.

Only one thread is used if a file is provided. If a folder is provided, takes all threads available. Use -J to specify a different number of threads.

Only provide a folder (resp. file) as a destination if a folder (resp. file) has been provided.
")
            .arg(arg!([SOURCE] "File/folder to compress. If a folder is provided, keeps arborescence and compresses up to a depth of 2.").required(true))
            .arg(arg!([DESTINATION] "File/folder to write to.").required(true))
            .arg(arg!(--del_src "If set, deletes source files as they are being compressed.").required(false))
            .arg(arg!(-J --num_threads <NUM_THREADS> "Number of threads to use (iif source is a folder). If 0, take all available").default_value("0").required(false))
    }

    fn run(matches: &ArgMatches) -> Result<(), Error>
    where
        Self: Sized,
    {
        let src: PathBuf = matches
            .value_of("SOURCE")
            .expect("Value of 'SOURCE' is required.")
            .into();
        let dst: PathBuf = matches
            .value_of("DESTINATION")
            .expect("Value of 'DESTINATION' is required.")
            .into();
        let del_src = matches.is_present("del_src");
        let num_threads: usize = matches
            .value_of("num_threads")
            .unwrap()
            .parse()
            .expect("'num_threads' has to be a number.");
        if src.is_file() {
            CompressDoc::compress_file(&src, &dst, del_src)?;
        } else if src.is_dir() {
            CompressDoc::compress_corpus(&src, &dst, del_src, num_threads)?;
        } else {
            return Err(
                std::io::Error::new(std::io::ErrorKind::NotFound, format!("{:?}", src)).into(),
            );
        }
        Ok(())
    }
}

/// impl block for helper functions related to [ExtractText].
//TODO: move into a proper op
impl OscarDoc {
    /// Extracts content from a Document.
    ///
    /// Fails if the `content` field is missing or is not a string.
    fn extract_from_doc(doc: &str) -> Result<String, Error> {
        let v: Value = serde_json::from_str(doc)?;

        if let Some(content) = v.get("content") {
            if let Value::String(c) = content {
                let mut content_str = c.to_string().replace(r#"\n"#, "\n");
                content_str.push('\n');
                Ok(content_str)
            } else {
                Err(Error::MalformedContent(v))
            }
        } else {
            Err(Error::MissingContent(v))
        }
    }

    fn extract<T: Read, U: Write>(src: T, dst: &mut U) -> Result<(), Error> {
        let b = BufReader::new(src);
        let docs = b.lines();
        for doc in docs {
            //extract and add newline
            let doc = doc?;
            let content = Self::extract_from_doc(&doc)? + "\n";
            let content_length = content.len();

            // check written bytes
            if dst.write(content.as_bytes())? > content_length {
                error!("IO Error: Could not write into destination writer.");
            }
        }

        // flush output
        dst.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::CompressDoc;
    use super::SplitDoc;
    use crate::ops::Split;
    use crate::{impls::OscarDoc, ops::Compress};
    use std::{
        fs::File,
        io::{Read, Write},
    };

    use tempfile::{self, tempdir};

    fn get_doc() -> &'static str {
        r#"{"content":"foo\nbar\nbaz\nquux"}
{"content":"123456789"}
{"content":"246810"}
{"content":"test"}"#
    }

    #[test]
    fn test_extract_single() {
        let docs = get_doc();
        let doc = docs.lines().next().unwrap().as_bytes();

        let mut buf = Vec::new();
        OscarDoc::extract(doc, &mut buf).unwrap();

        assert_eq!(String::from_utf8(buf).unwrap(), "foo\nbar\nbaz\nquux\n\n");
    }
    #[test]
    fn test_extract_multiple() {
        let doc = get_doc().as_bytes();
        let mut buf = Vec::new();
        OscarDoc::extract(doc, &mut buf).unwrap();

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "foo\nbar\nbaz\nquux\n\n123456789\n\n246810\n\ntest\n\n"
        );
    }
    #[test]
    fn extract_no_content() {
        let document = r#"{"no_content": "hehe"}"#;
        let extracted = OscarDoc::extract_from_doc(document);

        assert!(extracted.is_err())
    }

    #[test]
    fn extract_bad_content() {
        let document = r#"{"content": ["hehe"]}"#;
        let extracted = OscarDoc::extract_from_doc(document);

        assert!(extracted.is_err())
    }

    #[test]
    fn text_extract_from_doc() {
        let content = "foo
bar
baz
quux
";

        let document = r#"
        {
            "content":"foo\nbar\nbaz\nquux",
            "warc_headers":{
              "warc-block-digest":"sha1:X3OWP47FG2O5LBNMFSNB44FJF2SSRC26",
              "content-type":"text/plain",
              "warc-refers-to":"<urn:uuid:83f2e1d4-5ed3-41db-86ff-f7826c4c20f9>",
              "content-length":"16",
              "warc-identified-content-language":"eng",
              "warc-target-uri":"http://3dv2015.inria.fr/registration-2/index.html",
              "warc-date":"2021-09-16T11:07:14Z",
              "warc-record-id":"<urn:uuid:3304bc27-17d0-4ffd-a692-340381478a5f>",
              "warc-type":"conversion"
            },
            "metadata":{
              "identification":{
                "label":"en",
                "prob":0.6268374
              },
              "annotation":[
                "short_sentences",
                "footer"
              ],
              "sentence_identifications":[
                {
                  "label":"en",
                  "prob":0.93925816
                },
                null,
                {
                  "label":"en",
                  "prob":0.9937219
                },
                {
                  "label":"en",
                  "prob":0.9996538
                }
              ]
            }
          }
        "#;

        let extracted = OscarDoc::extract_from_doc(document).unwrap();
        assert_eq!(extracted, content);
    }

    pub fn setup_oscardoc() -> String {
        let mut corpus = String::new();
        for i in 0..10000 {
            corpus.push_str(&format!(r#"{{"item":{}}}"#, i));
            corpus.push('\n');
        }

        corpus
    }

    // the way of checking results is bad, since we merge then sort results
    // we should rather check the individual files one by one
    #[test]
    fn test_compress() {
        let content = setup_oscardoc();
        let content: Vec<&str> = content.lines().collect();
        let content_files = (&content).chunks(1000);
        let tmpdir = tempfile::tempdir().unwrap();
        for (idx, chunk) in content_files.enumerate() {
            // should be safe since it does not rely on rust destructor
            // + it is in a tempfile that will be cleaned at the exit of the test
            let tempfile_path = tmpdir.path().join(format!("file_{idx}.jsonl"));
            let mut tempfile = File::create(tempfile_path).unwrap();
            tempfile.write_all(chunk.join("\n").as_bytes()).unwrap();
        }

        // create destination path and compress
        let tmpdst = tempfile::tempdir().unwrap();
        CompressDoc::compress_folder(tmpdir.path(), tmpdst.path(), false).unwrap();

        println!(
            "{:?}",
            std::fs::read_dir(tmpdir.path())
                .unwrap()
                .collect::<Vec<_>>()
        );
        // let mut items_decompressed = Vec::new();

        let mut decompressed_data = Vec::new();
        for file in std::fs::read_dir(tmpdst.path()).unwrap() {
            println!("file: {:?}", file);
            // for file in split_files {
            let file = file.unwrap();
            let file = File::open(file.path()).unwrap();
            let mut reader = flate2::read::GzDecoder::new(file);
            let mut decompressed = String::new();
            reader.read_to_string(&mut decompressed).unwrap();
            decompressed_data.extend(decompressed.lines().map(|x| x.to_string()).into_iter());
        }

        // sort results
        decompressed_data.sort();
        let mut content = content;
        content.sort_unstable();
        assert_eq!(decompressed_data, content);
    }

    #[test]
    fn test_split_file() {
        let corpus = setup_oscardoc();

        // write corpus to file
        let test_dir = tempdir().unwrap();
        let corpus_orig = test_dir.path().join("corpus-orig.jsonl");
        let mut f = File::create(&corpus_orig).unwrap();
        f.write_all(corpus.as_bytes()).unwrap();

        // split
        let split_folder = test_dir.path().join("split");
        std::fs::create_dir(&split_folder).unwrap();

        let corpus_dst = split_folder.join("corpus-split.jsonl");
        SplitDoc::split_file(&corpus_orig, &corpus_dst, 1000).unwrap();

        let mut corpus_from_split = String::with_capacity(corpus.len());

        for file in std::fs::read_dir(split_folder).unwrap() {
            // for file in split_files {
            let file = file.unwrap();
            let split = std::fs::read_to_string(file.path()).unwrap();
            corpus_from_split.push_str(&split);
        }

        let mut from_split_corpus: Vec<&str> = corpus.lines().collect();
        from_split_corpus.sort_unstable();
        let mut from_split_list: Vec<&str> = corpus_from_split.lines().collect();
        from_split_list.sort_unstable();

        assert_eq!(from_split_corpus, from_split_list);
    }
}
