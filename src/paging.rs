use std::{ops::Range, io::{Result, Seek, Error, ErrorKind}, fs::File, collections::HashMap, cell::{RefCell, Ref}, rc::Rc, fmt::{Display}, mem::size_of};

use byteorder::{ReadBytesExt};

use crate::utils::{ReadableWritable, ReadStructurePos, WriteStructurePos};

pub const PAGE_SIZE: usize = 4096;
pub const BLOCK_SIZE: usize = 64;
pub const PAGE_BLOCK_COUNT: usize = 63;
pub const PAGE_PAYLOAD_SIZE: usize = BLOCK_SIZE * PAGE_BLOCK_COUNT;
pub const INVALID_BLOCK_INDEX: u8 = PAGE_BLOCK_COUNT as u8;
const INVALID_PAGE_INDEX: i32 = -1;
const MAX_PAGE_COUNT: i32 = i32::MAX;

#[repr(u8)]
enum BlockState {
    Free = 0,
    Busy = 1,
}

#[derive(Clone)]
struct Page {
    first_free_block: u8,
    block_states: [u8; PAGE_BLOCK_COUNT as usize],
    blocks: [u8; PAGE_PAYLOAD_SIZE],
}

impl Page {
    fn new() -> Page {
        Page {
            first_free_block: 0,
            block_states: [BlockState::Free as u8; PAGE_BLOCK_COUNT],
            blocks: [0; PAGE_PAYLOAD_SIZE],
        }
    }

    fn has_free_blocks(&self) -> bool {
        self.first_free_block != INVALID_BLOCK_INDEX
    }

    fn get_block_data(&self, index: u8, offset: usize, length: usize) -> &[u8] {
        &self.blocks[Page::get_block_data_range(index, offset, length)]
    }

    fn set_block_data(&mut self, index: u8, data: &[u8], offset: usize) -> bool {
        let block_data = &mut self.blocks[Page::get_block_data_range(index, offset, data.len())];
        if block_data.eq(&data) {
            return false;
        }

        block_data.copy_from_slice(&data);
        self.block_states[index as usize] = BlockState::Busy as u8;

        if index != self.first_free_block {
            return true;
        }

        for i in index as usize..PAGE_BLOCK_COUNT {
            if (BlockState::Free as u8) == self.block_states[i] {
                self.first_free_block = i as u8;
                return true;
            }
        }

        self.first_free_block = INVALID_BLOCK_INDEX;
        true
    }

    fn get_block_data_range(index: u8, offset: usize, length: usize) -> Range<usize> {
        if index >= PAGE_BLOCK_COUNT as u8 {
            panic!("Invalid block index {:?}", index)
        }

        let length = if length > 0 { length } else { BLOCK_SIZE };

        if offset + length > BLOCK_SIZE {
            panic!("Offset + Length can't be greater than block size {:?}", BLOCK_SIZE)
        }

        let start = index as usize * BLOCK_SIZE + offset;
        start..start + length
    }
}

impl ReadableWritable for Page {
    fn read_to_buffer(read_action: impl FnOnce(&mut [u8]) -> Result<Self>) -> Result<Self> {
        let mut buffer = [0; size_of::<Self>()];
        read_action(&mut buffer)
    }
}

#[derive(Clone, Copy, PartialEq)]
#[repr(align(2))]
pub struct BlockAddress {
    pub page_index: i32,
    pub block_index: u8,
}

impl BlockAddress {
    pub const fn invalid() -> Self {
        BlockAddress {
            page_index: INVALID_PAGE_INDEX,
            block_index: INVALID_BLOCK_INDEX,
        }
    }

    pub fn new(page_index: i32, block_index: u8) -> Self {
        BlockAddress { page_index, block_index }
    }

    pub const fn size_in_buffer() -> usize {
        size_of::<BlockAddress>()
    }
}

impl Default for BlockAddress {
    fn default() -> Self {
        BlockAddress::invalid()
    }
}

impl Display for BlockAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if *self == BlockAddress::invalid() {
            f.write_str("Invalid")?;
        }
        else {
            f.write_fmt(format_args!("P: {:?}, B: {:?}", self.page_index, self.block_index))?;
        }

        Ok(())
    }
}

impl ReadableWritable for BlockAddress {
    fn read_to_buffer(read_action: impl FnOnce(&mut [u8]) -> Result<Self>) -> Result<Self> {
        let mut buffer = [0; size_of::<Self>()];
        read_action(&mut buffer)
    }
}

#[derive(Default, Clone)]
#[repr(align(4))]
struct PagesHeader {
    first_page_with_free_blocks: i32,
}

impl ReadableWritable for PagesHeader {
    fn read_to_buffer(read_action: impl FnOnce(&mut [u8]) -> Result<Self>) -> Result<Self> {
        let mut buffer = [0; size_of::<Self>()];
        read_action(&mut buffer)
    }
}

pub struct PageManager {
    imp: Rc<RefCell<PageManagerImpl>>,
}

impl PageManager {
    pub fn new(file: Rc<RefCell<File>>, offset: u64) -> Result<Self> {
        Ok(PageManager { imp: Rc::new(RefCell::new(PageManagerImpl::new(file, offset)?)) })
    }

    pub fn get_page(&mut self, index: i32) -> Result<PageAccessor> {
        let mut imp_mut = self.imp.as_ref().borrow_mut();
        Ok(PageAccessor {
            page_manager: self.imp.clone(),
            page: imp_mut.get_page(index)?,
            index: index,
            has_changes: false
        })
    }

    pub fn get_page_with_free_blocks(&mut self, start_index: i32) -> Result<PageAccessor> {
        let index = self.imp.borrow_mut().find_page_with_free_blocks(start_index)?;
        self.get_page(index)
    }
}

struct PageManagerImpl {
    file: Rc<RefCell<File>>,
    header_offset: u64,
    first_page_offset: u64,
    header: PagesHeader,
    cached_pages: HashMap<i32, Rc<RefCell<Page>>>,
}

impl PageManagerImpl {
    fn new(file: Rc<RefCell<File>>, offset: u64) -> Result<Self> {
        let pages_header = if file.borrow().metadata()?.len() <= offset {
            PagesHeader::default()
        }
        else {
            file.borrow_mut().read_structure_from_pos(offset)?
        };

        let first_page_offset = offset + PagesHeader::size_in_buffer() as u64;

        Ok(PageManagerImpl { file, header_offset: offset, first_page_offset, header: pages_header, cached_pages: HashMap::new() })
    }

    fn get_page(&mut self, index: i32) -> Result<Rc<RefCell<Page>>> {
        if index < 0 || index >= MAX_PAGE_COUNT {
            panic!("Invalid page index {:?}", index);
        }

        if let Some(p) = self.cached_pages.get(&index) {
            Ok(p.clone())
        }
        else {
            let page_address = self.get_page_address(index);
            let new_page = if self.file.borrow().metadata()?.len() <= page_address {
                Page::new()
            }
            else {
                self.file.borrow_mut().read_structure_from_pos(page_address)?
            };

            let page = Rc::new(RefCell::new(new_page));
            let cloned_page = page.clone();
            self.cached_pages.insert(index, page);
            Ok(cloned_page)
        }
    }

    fn commit_page(&mut self, index: i32, page: &Page) -> Result<()> {
        self.file.borrow_mut().write_structure_to_pos(self.get_page_address(index), page)?;

        if index == self.header.first_page_with_free_blocks && !page.has_free_blocks() {
            let index = self.find_page_with_free_blocks(index + 1)?;
            self.update_first_page_with_free_blocks(index)?;
        }
        else if page.has_free_blocks() && index < self.header.first_page_with_free_blocks {
            self.update_first_page_with_free_blocks(index)?;
        }

        Ok(())
    }

    fn get_page_address(&self, index: i32) -> u64 {
        self.first_page_offset + (index as usize * PAGE_SIZE) as u64
    }

    fn update_first_page_with_free_blocks(&mut self, index: i32) -> Result<()> {
        self.header.first_page_with_free_blocks = index;
        self.file.borrow_mut().write_structure_to_pos(self.header_offset, &self.header)
    }

    fn find_page_with_free_blocks(&mut self, start: i32) -> Result<i32> {
        for index in start..MAX_PAGE_COUNT {
            if let Some(page) = self.cached_pages.get(&index) {
                if page.as_ref().borrow().has_free_blocks() { return Ok(index); }
            }

            let page_address = self.get_page_address(index);
            if self.file.borrow().metadata()?.len() <= page_address {
                return Ok(index);
            }

            self.file.borrow_mut().seek(std::io::SeekFrom::Start(page_address))?;
            if self.file.borrow_mut().read_u8()? != INVALID_BLOCK_INDEX {
                return Ok(index);
            }
        }

        Err(Error::new(ErrorKind::NotFound, "Couldn't find a page with free blocks"))
    }
}

pub struct PageAccessor {
    page_manager: Rc<RefCell<PageManagerImpl>>,
    page: Rc<RefCell<Page>>,
    index: i32,
    has_changes: bool,
}

impl PageAccessor {
    pub fn get_block_data(&self, index: u8, offset: usize, length: usize) -> Ref<[u8]> {
        Ref::map(self.page.as_ref().borrow(), |p| p.get_block_data(index, offset, length))
    }

    pub fn set_block_data(&mut self, index: u8, data: &[u8], offset: usize) {
        self.has_changes = self.page.as_ref().borrow_mut().set_block_data(index, data, offset) || self.has_changes;
    }

    pub fn has_free_blocks(&self) -> bool {
        self.page.borrow().has_free_blocks()
    }

    pub fn first_free_block(&self) -> u8 {
        self.page.borrow().first_free_block
    }

    pub fn index(&self) -> i32 {
        self.index
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.has_changes {
            return self.page_manager.borrow_mut().commit_page(self.index, &mut *self.page.borrow_mut())
        }

        Ok(())
    }
}

impl Drop for PageAccessor {
    fn drop(&mut self) {
        self.commit().unwrap();
    }
}