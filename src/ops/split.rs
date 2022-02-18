/*! Splitting operations.

 These operations split the corpus into smaller files of a defined max size.
!*/
use std::{
    borrow::Cow,
    fs::File,
    io::{BufRead, BufReader, ErrorKind, Write},
    path::{Path, PathBuf},
};

use crate::error::Error;
use rayon::iter::{ParallelBridge, ParallelIterator};

/// Rotating file writer.
///
/// Files are named `foo.bar`, and if there is a need of more than one file,
/// `foo.bar` is renamed `foo_part_1.bar`, and so on.
struct SplitWriter {
    dst: PathBuf,
    fp: Option<File>,
    max_size: usize,
    current_size: usize,
    nb_files: u32,
}

impl SplitWriter {
    /// Create a new writer. `max_size` is in bytes.
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
    /// Get the next filename **and** bump `self.nb_files`
    fn next_filename(&mut self) -> Option<Cow<Path>> {
        if self.nb_files == 0 {
            self.nb_files += 1;
            Some(Cow::from(&self.dst))
        } else if let (Some(stem), Some(extension)) = (self.dst.file_stem(), self.dst.extension()) {
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

    /// Close current file and open a new one
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
    /// Assumes all corpus files to be in the same dict (not in separate folders)
    fn split_all(
        src: &Path,
        dst: &Path,
        split_size: usize,
        num_threads: usize,
    ) -> Result<(), Error> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()?;
        debug!("Built rayon threadpool with num_threads={num_threads}");

        //check existence of folder and/or its emptyness
        // if dst.exists() {
        //     return Err(std::io::Error::new(ErrorKind::AlreadyExists, format!("{:?}", dst)).into());
        // }
        // if dst.is_file() {
        //     // when #86442 is merged
        //     //return Err(std::io::Error::new(ErrorKind::NotADirectory, format!("{:?}", dst)).into());
        //     return Err(Error::Custom("Not a directory".to_string()));
        // }

        let files = std::fs::read_dir(src)?;

        if !dst.exists() {
            debug!("{:?} does not exist, creating.", dst);
            std::fs::create_dir(dst)?;
        }

        if dst.read_dir()?.count() != 0 {
            error!("Destination directory is not empty!");
            return Err(std::io::Error::new(ErrorKind::AlreadyExists, format!("{:?}", dst)).into());
        }

        // filter out folders and errors (printing then discarding them)
        let files = files
            .filter_map(|p| match p {
                Ok(path) => {
                    let path = path.path();
                    if path.is_file() {
                        Some(path)
                    } else {
                        None
                    }
                }
                Err(e) => {
                    error!("Discarding the following path due to an error: {:?}", e);
                    None
                }
            })
            .par_bridge();

        debug!("got: {:#?}", files);

        let r: Vec<Result<(), Error>> = files
            .map(|file| {
                // extract filename
                // send to a split file
                if let Some(filename) = file.file_stem() {
                    // Create folder for file
                    let dest_folder: PathBuf = [dst.as_os_str(), filename].iter().collect();
                    std::fs::create_dir(&dest_folder)?;

                    // create base file name
                    let mut dest_file = dest_folder.clone();
                    let file_name = file.file_name().unwrap();
                    dest_file.push(file_name);

                    debug!("Splitting {:?} in {:?}", file, dest_folder);
                    Self::split_file(&file, &dest_file, split_size)?;
                    debug!("Done      {:?} in {:?}", file, dest_folder);
                };
                Ok(())
            })
            .collect();

        // Collect eventual errors
        let errors: Vec<Error> = r.into_iter().filter_map(|result| result.err()).collect();

        if errors.is_empty() {
            Ok(())
        } else {
            // print errors
            for e in errors {
                error!("{:?}", e);
            }
            Err(Error::Custom(
                "Error(s) during splitting. Check logs.".to_string(),
            ))
        }
    }

    /// Split a single file into multiple fixed size ones
    fn split_file(src: &Path, dst: &Path, split_size: usize) -> Result<(), Error> {
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
