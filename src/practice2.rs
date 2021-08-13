use serde_json::Deserializer;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use failure::Fail;
use serde::{Deserialize, Serialize};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

// command/entry type stored in db
#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }
    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

// kv store struct
pub struct KvStore {
    // directory for the data and log
    path: PathBuf,
    // writer of current log
    writer: BufWriterWithPos<File>,
    // readers map the gen_id to specific file reader
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // map command to real position
    index_map: BTreeMap<String, CommandPos>,
    // the stale data size need be compacted
    uncompacted: u64,
    // current gen_id
    current_gen: u64,
}

impl KvStore {
    // initial based on specific path
    // it will creat a new one if the path does not exist
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let mut readers = HashMap::new();
        let mut index_map = BTreeMap::new();
        let mut uncompacted = 0;
        let gen_list = sorted_generation_list(&path)?;
        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &mut index_map)?;
            readers.insert(gen, reader);
        }
        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers)?;
        Ok(Self {
            path,
            writer,
            readers,
            index_map,
            uncompacted,
            current_gen,
        })
    }

    // set a string value of the given key
    // if the key exists, the value will be overwritten
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .index_map
                .insert(key, (self.current_gen, pos..self.writer.pos).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    // get the value of given key
    // if the key does not exist, it will return `None`.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index_map.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    // remove the given key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index_map.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = self.index_map.remove(&key).expect("Key not found");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    // clear stale data in the log
    pub fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = self.new_log_file(self.current_gen)?;

        let mut writer = self.new_log_file(compaction_gen)?;
        let mut new_pos = 0;
        for cmd_pos in self.index_map.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut entry_reader, &mut writer)?;
            *cmd_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }

        writer.flush()?;
        let stales_gens = self
            .readers
            .keys()
            .filter(|&&k| k < compaction_gen)
            .cloned()
            .collect::<Vec<_>>();
        for gen in stales_gens {
            self.readers.remove(&gen);
            fs::remove_file(log_path(&self.path, gen))?;
        }
        self.uncompacted = 0;
        Ok(())
    }

    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
    }
}

fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, gen);
    let writer = BufWriterWithPos::new(OpenOptions::new().create(true).append(true).open(&path)?)?;
    readers.insert(gen, BufReaderWithPos::new(File::open(path)?)?);
    Ok(writer)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

fn sorted_generation_list(path: &Path) -> Result<Vec<u64>> {
    let mut generation_list = fs::read_dir(path)?
        .flat_map(|s| -> Result<_> { Ok(s?.path()) })
        .filter(|p| p.is_file() && p.extension() == Some("log".as_ref()))
        .flat_map(|p| {
            p.file_name()
                .and_then(OsStr::to_str)
                .map(|name| name.trim_end_matches(".log"))
                .map(|x| x.parse::<u64>())
        })
        .flatten()
        .collect::<Vec<_>>();
    generation_list.sort_unstable();
    Ok(generation_list)
}

fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index_map: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut uncompacted = 0;
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut s = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = s.next() {
        let new_pos = s.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index_map.insert(key, (gen, (pos..new_pos)).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key, .. } => {
                if let Some(old_cmd) = index_map.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        Self {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos += self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    IOError(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    SerdeError(#[cause] serde_json::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvsError {
    fn from(err: Error) -> Self {
        KvsError::IOError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> Self {
        KvsError::SerdeError(err)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
