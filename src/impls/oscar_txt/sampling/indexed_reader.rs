use std::io::{BufRead, Seek};
/// Reader over corpus that skips to provided offsets and returns [Deref<Target=str>].
/// inner is a reader over a corpus
/// indices is an iterator over the line offsets to keep
pub struct IndexedReader<R: BufRead+Seek, I: Iterator<Item=u64>> {
    inner: R,
    indices: I
}

impl<R: BufRead+Seek, I: Iterator<Item=u64>> IndexedReader<R, I> {
    pub fn new(inner: R, indices: I) -> Self {
        Self {inner, indices}
    } 
}

impl<R: BufRead+Seek, I: Iterator<Item=u64>> Iterator for IndexedReader<R, I> {
    type Item = std::io::Result<String>; 

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next_offset) = self.indices.next() {
            // seek to the good offset, early return if IO error
            //TODO: Check behaviour consistency on uot of bounds seeking
            if let Err(e) = self.inner.seek(std::io::SeekFrom::Start(next_offset)) {
                return Some(Err(e));
            };
            let mut ret = String::new();
            // Read the line, early return if IO error
            if let Err(e) = self.inner.read_line(&mut ret) {
                return Some(Err(e));
            };

            // Check if ret is empty (out of bounds?), and end iterator if it is
            if ret.is_empty() {
                None
            } else {
                Some(Ok(ret))
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::IndexedReader;
    #[test]
    fn test_indexed_reader() {
        let lines = vec![
            "foo",
            "bar ij",
            r#"baz  \ni"#,
            "quux"
        ];
        let text = lines.join("\n");

        let mut c = Cursor::new(text);
        let mut i = [0, 11, 20].into_iter();
        let it = IndexedReader::new(&mut c, &mut i);
        for (idx, indexed_string) in it.enumerate() {
            println!("{indexed_string:?}");
            match idx {
                0 => assert_eq!(indexed_string.unwrap().trim(), lines[0]),
                1 => assert_eq!(indexed_string.unwrap().trim(), lines[2]),
                2 => assert_eq!(indexed_string.unwrap().trim(), lines[3]),
                _ => panic!("too far away!")
            }
        }
    }

    #[test]
    fn test_indexed_reader_out_of_bounds() {
        let lines = vec![
            "foo",
            "bar ij",
            r#"baz  \ni"#,
            "quux"
        ];
        let text = lines.join("\n");

        let mut c = Cursor::new(text);
        let mut i = [0, 2, 3000, 3000].into_iter();
        let it = IndexedReader::new(&mut c, &mut i);
        let strings : std::io::Result<Vec<_>>= it.collect();
        println!("{strings:?}");
        assert_eq!(strings.unwrap().len(), 2);
    }
}