use std::path::PathBuf;

use clap::{arg, ArgMatches};

use crate::{cli::Command, error::Error, ops::Compress};

/// internal struct for compression op implementation
pub struct CompressDoc;
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
            .arg(arg!(--compression <COMP> "Compression to use (gzip, zstd)").required(false).default_value("zstd"))
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
        let compression = matches.value_of("compression").unwrap();
        let num_threads: usize = matches
            .value_of("num_threads")
            .unwrap()
            .parse()
            .expect("'num_threads' has to be a number.");
        if src.is_file() {
            CompressDoc::compress_file(&src, &dst, del_src, compression)?;
        } else if src.is_dir() {
            CompressDoc::compress_corpus(&src, &dst, del_src, compression, num_threads)?;
        } else {
            return Err(
                std::io::Error::new(std::io::ErrorKind::NotFound, format!("{:?}", src)).into(),
            );
        }
        Ok(())
    }
}
