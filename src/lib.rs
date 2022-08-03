use std::{io::{Result, Read, Write}, fs::{OpenOptions, File}, rc::Rc, cell::RefCell, mem::size_of};

use paging::{BlockAddress, PageManager};
use read_write::{PageReader, PageWriter};
use utils::{ReadableWritable, ReadStructure, WriteStructure, WriteStructurePos, ReadStructurePos, ArrayStructReaderWriter};

mod paging;
mod utils;
mod read_write;

pub struct Database {
    file: Rc<RefCell<File>>,
    page_manager: PageManager,
    system_info: DbSystemInfo,
    key_buffer: Vec<u8>,
}

impl Database {
    pub fn new(path: &str) -> Result<Self> {
        let file = Rc::new(RefCell::new(
            OpenOptions::new().create(true).read(true).write(true).open(path)?));
        let page_manager = PageManager::new(file.clone(), DbSystemInfo::size_in_buffer() as u64)?;
        let mut db = Database {
            file: file.clone(),
            page_manager,
            system_info: DbSystemInfo::default(),
            key_buffer: vec![0; 32],
        };
        if file.borrow().metadata()?.len() == 0 {
            db.initialize()?;
        }

        db.read_system_info()?;

        Ok(db)
    }

    fn initialize(&mut self) -> Result<()> {
        self.system_info = DbSystemInfo::default();
        self.write_system_info()?;
        Ok(())
    }

    pub fn set(&mut self, key: &str, data: &[u8]) {
        let key_bytes = key.as_bytes();
        if let Some(_) = self.find(key_bytes) {
            return;
        }

        let new_record_address = {
            let mut page_writer = PageWriter::new(&mut self.page_manager).unwrap();
            page_writer
                .write_structure(&RecordHeader {
                    next_record: BlockAddress::invalid(),
                    key_size: key_bytes.len() as i32,
                    data_size: data.len() as i32
                })
                .unwrap();

            page_writer.write_all(key_bytes).unwrap();
            page_writer.write_all(data).unwrap();
            page_writer.start_address()
        };

        if self.system_info.last_record != BlockAddress::invalid() {
            let mut page = self.page_manager.get_page(self.system_info.last_record.page_index).unwrap();
            let block_index = self.system_info.last_record.block_index;
            let header = page.get_block_data(block_index, 0, RecordHeader::size_in_buffer())
                .read_structure::<RecordHeader>();
            let mut buffer = [0 as u8; RecordHeader::size_in_buffer()];
            buffer.write_structure(&RecordHeader { next_record: new_record_address, key_size: header.key_size, data_size: header.data_size });
            page.set_block_data(block_index, &buffer, 0);
        }

        self.system_info.last_record = new_record_address;
        if self.system_info.first_record == BlockAddress::invalid() {
            self.system_info.first_record = new_record_address;
        }

        self.write_system_info().unwrap();
    }

    pub fn get(&mut self, key: &str) -> Option<Vec<u8>> {
        if let Some((header, address)) = self.find(key.as_bytes()) {
            let mut reader = PageReader::new(&mut self.page_manager, address).unwrap();
            reader.skip(RecordHeader::size_in_buffer() + header.key_size as usize).unwrap();
            let mut result = vec![0; header.data_size as usize];
            reader.read_exact(&mut result).unwrap();
            Some(result)
        }
        else {
            None
        }
    }

    pub fn get_to_buffer(&mut self, key: &str, buffer: &mut [u8]) -> bool {
        if let Some((header, address)) = self.find(key.as_bytes()) {
            if buffer.len() < header.data_size as usize {
                panic!("123");
            }

            let mut reader = PageReader::new(&mut self.page_manager, address).unwrap();
            reader.skip(RecordHeader::size_in_buffer() + header.key_size as usize).unwrap();
            reader.read(buffer).unwrap();
            true
        }
        else {
            false
        }
    }

    fn find(&mut self, key_bytes: &[u8]) -> Option<(RecordHeader, BlockAddress)> {
        if self.system_info.first_record == BlockAddress::invalid() {
            return None;
        }

        let mut record_address = self.system_info.first_record;
        while record_address != BlockAddress::invalid() {
            let mut reader = PageReader::new(&mut self.page_manager, record_address).unwrap();
            let record_header = reader.read_structure::<RecordHeader>().unwrap();

            let key_size = record_header.key_size as usize;
            if key_size == key_bytes.len() {
                if self.key_buffer.len() < key_size {
                    self.key_buffer.resize(key_size, 0);
                }

                let key_slice = &mut self.key_buffer[0..key_size];
                reader.read_exact(key_slice).unwrap();

                if key_slice.eq(&key_bytes) {
                    return Some((record_header, record_address));
                }
            }

            record_address = record_header.next_record;
        }

        None
    }

    fn read_system_info(&mut self) -> Result<()> {
        self.system_info = self.file.borrow_mut().read_structure_from_pos(0)?;
        Ok(())
    }

    fn write_system_info(&mut self) -> Result<()> {
        self.file.borrow_mut().write_structure_to_pos(0, &self.system_info)?;
        Ok(())
    }
}

#[derive(Default, Clone)]
struct DbSystemInfo {
    first_record: BlockAddress,
    last_record: BlockAddress,
}

impl ReadableWritable for DbSystemInfo {
    fn read_to_buffer(read_action: impl FnOnce(&mut [u8]) -> Result<Self>) -> Result<Self> {
        let mut buffer = [0; size_of::<Self>()];
        read_action(&mut buffer)
    }
}

#[derive(Clone)]
struct RecordHeader {
    next_record: BlockAddress,
    key_size: i32,
    data_size: i32,
}

impl RecordHeader {
    const fn size_in_buffer() -> usize {
        size_of::<RecordHeader>()
    }
}

impl ReadableWritable for RecordHeader {
    fn read_to_buffer(read_action: impl FnOnce(&mut [u8]) -> Result<Self>) -> Result<Self> {
        let mut buffer = [0; size_of::<Self>()];
        read_action(&mut buffer)
    }
//     fn size_in_buffer() -> usize {
//         RecordHeader::size_in_buffer()
//     }

//     fn read(reader: &mut impl std::io::Read) -> Result<Self> {
//         let next_record = reader.read_structure()?;
//         let key_size = reader.read_i32::<LittleEndian>()?;
//         let data_size = reader.read_i32::<LittleEndian>()?;

//         Ok(RecordHeader { next_record, key_size, data_size })
//     }

//     fn write(&self, writer: &mut impl std::io::Write) -> Result<()> {
//         writer.write_structure(&self.next_record)?;
//         writer.write_i32::<LittleEndian>(self.key_size)?;
//         writer.write_i32::<LittleEndian>(self.data_size)?;

//         Ok(())
//     }
}