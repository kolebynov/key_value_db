use std::{io::{Read, Write, Seek, Result, SeekFrom, Cursor}, mem::{size_of}, slice};

pub trait ReadableWritable : Sized + Clone {
    fn size_in_buffer() -> usize {
        size_of::<Self>()
    }

    unsafe fn read(reader: &mut impl Read) -> Result<Self> {
        Self::read_from_buffer(|buffer| {
            reader.read_exact(buffer)?;
            Ok(buffer.as_ptr().cast::<Self>().as_ref().unwrap().clone())
        })
    }

    unsafe fn write(&self, writer: &mut impl Write) -> Result<()> {
        let slice = slice::from_raw_parts((self as *const Self) as *const u8, size_of::<Self>());
        writer.write_all(slice)?;
        Ok(())
    }

    fn read_from_buffer(read_action: impl FnOnce(&mut [u8]) -> Result<Self>) -> Result<Self>;
}

pub trait ReadStructure : Read + Sized {
    fn read_structure<T: ReadableWritable>(&mut self) -> Result<T> {
        unsafe { T::read(self) }
    }
}

pub trait ReadStructurePos : Read + Seek + Sized {
    fn read_structure_from_pos<T: ReadableWritable>(&mut self, position: u64) -> Result<T> {
        self.seek(SeekFrom::Start(position))?;
        unsafe { T::read(self) }
    }
}

pub trait WriteStructure : Write + Sized {
    fn write_structure<T: ReadableWritable>(&mut self, structure: &T) -> Result<()> {
        unsafe { structure.write(self) }
    }
}

pub trait WriteStructurePos : Write + Seek + Sized {
    fn write_structure_to_pos<T: ReadableWritable>(&mut self, position: u64, structure: &T) -> Result<()> {
        self.seek(SeekFrom::Start(position))?;
        unsafe { structure.write(self) }
    }
}

impl<R: Read + Sized> ReadStructure for R {}

impl<R: Read + Seek + Sized> ReadStructurePos for R {}

impl<W: Write + Sized> WriteStructure for W {}

impl<W: Write + Seek + Sized> WriteStructurePos for W {}

pub trait ArrayStructReaderWriter {
    fn read_structure<T: ReadableWritable>(&self) -> T;

    fn write_structure<T: ReadableWritable>(&mut self, structure: &T);
}

impl ArrayStructReaderWriter for [u8] {
    fn read_structure<T: ReadableWritable>(&self) -> T {
        if self.len() < T::size_in_buffer() {
            panic!("Buffer can't be less than structure size");
        }

        let mut cursor = Cursor::new(self);
        unsafe { T::read(&mut cursor) }.unwrap()
    }

    fn write_structure<T: ReadableWritable>(&mut self, structure: &T) {
        if self.len() < T::size_in_buffer() {
            panic!("Buffer can't be less than structure size");
        }

        let mut cursor = Cursor::new(self);
        unsafe { structure.write(&mut cursor) }.unwrap();
    }
}