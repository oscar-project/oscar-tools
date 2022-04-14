use std::{
    default,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

use clap::arg;
use runiq::filters::{DigestFilter, Filter};

use crate::{cli::Command, error::Error, ops::Dedup};

// #[derive(Default)]
pub struct DedupTxt {
    filter: Box<dyn Filter>,
}

impl DedupTxt {
    fn new(filter: Box<dyn Filter>) -> Self {
        Self { filter }
    }

    /// get the input from the reader, deduplicate it and send it to the writer.
    /// Stops at the end of stream
    /// Use a [BufWriter] to have better performance.
    fn dedup<R, W>(&mut self, r: &mut R, w: &mut W) -> Result<(), Error>
    where
        R: BufRead,
        W: Write,
    {
        for line in r.lines() {
            let line = line?;
            let line_bytes = line.as_bytes();
            // check if line is a newline between documents
            if line == "\n" {
                w.write(b"\n")?;
            } else if self.filter.detect(line_bytes) {
                // write iif line is detected by filter as a unique, never seen line
                w.write(line_bytes)?;
                w.write(b"\n")?;
            }
        }

        w.flush()?;
        Ok(())
    }
}

impl Dedup for DedupTxt {
    fn dedup(&mut self, src: &std::path::Path, dst: &std::path::Path) -> Result<(), Error> {
        let r = File::open(&src)?;
        let w = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&dst)?;
        let mut br = BufReader::new(r);
        let mut bw = BufWriter::new(w);

        self.dedup(&mut br, &mut bw)
    }
}

impl Command for DedupTxt {
    fn subcommand() -> clap::App<'static>
    where
        Self: Sized,
    {
        clap::App::new("dedup")
            .about("line deduplication")
            .arg(arg!([SOURCE] "Corpus source file."))
            .arg(arg!([DESTINATION] "Corpus destination file. Should not exist."))
    }

    fn run(matches: &clap::ArgMatches) -> Result<(), Error>
    where
        Self: Sized,
    {
        let src: PathBuf = matches.value_of("SOURCE").unwrap().into();
        let dst: PathBuf = matches.value_of("DESTINATION").unwrap().into();

        let mut d = Self::default();
        // not sure of the syntax here...
        // X as Y makes us "see" the struct X as the trait Y, so that we can
        // disambiguate on similarly named methods.
        <DedupTxt as Dedup>::dedup(&mut d, &src, &dst)?;
        Ok(())
    }
}

impl Default for DedupTxt {
    fn default() -> Self {
        Self {
            filter: Box::new(DigestFilter::default()),
        }
    }
}
#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::DedupTxt;

    #[test]
    fn test_simple() {
        let data = "foo
bar
baz
quux
baz
baz
zoom";
        let expected = "foo
bar
baz
quux
zoom
";

        let mut dedup = DedupTxt::default();

        let mut dest = Vec::new();
        let mut r = Cursor::new(&data);
        dedup.dedup(&mut r, &mut dest).unwrap();

        let result = String::from_utf8_lossy(&dest);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_multi_doc() {
        let data = "foo
bar
baz
quux
baz
baz
zoom

doc2
hey
foo
newline
never seen

never seen again
baz
zoom
hoop
last document is only duplicates :o

foo
bar
baz
zoom";
        let expected = "foo
bar
baz
quux
zoom

doc2
hey
newline
never seen
never seen again
hoop
last document is only duplicates :o
";

        let mut dedup = DedupTxt::default();

        let mut dest = Vec::new();
        let mut r = Cursor::new(&data);
        dedup.dedup(&mut r, &mut dest).unwrap();

        let result = String::from_utf8_lossy(&dest);
        assert_eq!(result, expected);
    }
}
