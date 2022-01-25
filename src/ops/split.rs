/*! Splitting operations
!*/
use std::{
    borrow::Cow,
    fmt::format,
    fs::File,
    io::{BufRead, BufReader, BufWriter, ErrorKind, Write},
    path::{Path, PathBuf},
};

use crate::error::Error;

struct SplitWriter {
    dst: PathBuf,
    fp: Option<File>,
    max_size: usize,
    current_size: usize,
    nb_files: u32,
}

impl SplitWriter {
    pub fn new(dst: &Path, max_size: usize) -> Self {
        Self {
            dst: dst.to_path_buf(),
            fp: None,
            max_size,
            current_size: 0,
            nb_files: 0,
        }
    }

    /// transforms foo.bar into foo_part_<part_number>.bar
    #[inline]
    fn format_filename(filename: &Path, part_number: u64) -> Option<PathBuf> {
        if let (Some(stem), Some(extension)) = (filename.file_stem(), filename.extension()) {
            // clone filename
            let mut next_filename = filename.to_path_buf();

            // get stem and forge new filename
            let mut file_stem = stem.to_os_string();
            file_stem.push(format!("_part_{}", part_number));
            next_filename.set_file_name(file_stem);
            next_filename.set_extension(extension);

            Some(next_filename)
        } else {
            None
        }
    }

    // TODO: return error if no stem/extension
    fn next_filename(&mut self) -> Option<Cow<Path>> {
        if self.nb_files == 0 {
            self.nb_files += 1;
            Some(Cow::from(&self.dst))
        } else {
            if let (Some(stem), Some(extension)) = (self.dst.file_stem(), self.dst.extension()) {
                // clone filename
                let mut next_filename = self.dst.clone();

                // get stem and forge new filename
                let mut file_stem = stem.to_os_string();
                file_stem.push(format!("_part_{}", self.nb_files));
                next_filename.set_file_name(file_stem);
                next_filename.set_extension(extension);

                println!("{:?}", next_filename);

                // increase file count
                self.nb_files += 1;

                Some(Cow::from(next_filename))
            } else {
                None
            }
        }
    }

    pub fn rotate_file(&mut self) -> std::io::Result<()> {
        if self.nb_files == 1 {
            // moving foo.bar to foo_part_1.bar
            let new_filename = Self::format_filename(&self.dst, 1).unwrap();

            // early return if filename exists
            if new_filename.exists() {
                return Err(std::io::Error::new(
                    ErrorKind::AlreadyExists,
                    format!("{:?}", new_filename),
                ));
            } else {
                debug!("moving {:?} to {:?}", self.dst, new_filename);
                self.fp = None;
                self.nb_files += 1;
                std::fs::rename(&self.dst, new_filename)?;
            }
        }

        let filename = self.next_filename().unwrap();

        if filename.exists() {
            Err(std::io::Error::new(
                ErrorKind::AlreadyExists,
                format!("{:?}", filename),
            ))
        } else {
            debug!("Rotating: creating {:?}", filename);
            self.fp = Some(File::create(&filename)?);
            self.current_size = 0;
            Ok(())
        }
    }
}

impl Write for SplitWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // create first file if fp is none
        if self.fp.is_none() {
            self.rotate_file()?;
        }

        // create new split if current is full
        if self.current_size + buf.len() > self.max_size {
            self.rotate_file()?;
        }

        // print warning if buf very large
        if buf.len() > self.max_size {
            warn!("Current entry is too large: Split size limits won't be enforced (entry size: {}, max size:{}", buf.len(), self.max_size);
        }

        if let Some(fp) = &mut self.fp {
            let bytes_written = fp.write(buf)?;
            self.current_size += bytes_written;
            Ok(bytes_written)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No file to write to.",
            ))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(fp) = &mut self.fp {
            fp.flush()
        } else {
            Ok(())
        }
    }
}
pub trait Split {
    /// Split a single file into multiple fixed size ones
    fn split(src: &Path, dst: &Path, split_size: usize) -> Result<(), Error> {
        debug!("Using default splitter with size {split_size}");
        let corpus = File::open(&src)?;
        let corpus_buf = BufReader::new(corpus);
        let documents = corpus_buf.lines();

        let mut split_writer = SplitWriter::new(dst, split_size);
        for document in documents {
            let mut document = document?;
            document.push('\n');
            split_writer.write_all(document.as_bytes())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::SplitWriter;

    #[test]
    fn file_name_single() {
        let p = PathBuf::from("foo.txt");
        let mut s = SplitWriter::new(&p, 100);
        assert_eq!(s.next_filename().unwrap(), p);
    }

    #[test]
    fn file_name_multiple() {
        let p = PathBuf::from("foo.txt");
        let expected = PathBuf::from("foo_part_3.txt");
        let mut s = SplitWriter::new(&p, 100);
        s.next_filename();
        s.next_filename();
        s.next_filename();
        let res = s.next_filename();
        assert_eq!(res.unwrap(), expected);
    }
}
