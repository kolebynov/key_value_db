use std::io::{Write, Read, Result, Error};

use crate::{paging::{PageManager, BlockAddress, PageAccessor, BLOCK_SIZE}, utils::{ArrayStructReaderWriter}};

const BLOCK_DATA_SIZE: usize = BLOCK_SIZE - BlockAddress::size_in_buffer();

pub struct PageReader<'a> {
    page_manager: &'a mut PageManager,
    current_page: PageAccessor,
    block_index: u8,
    block_offset: usize
}

impl<'a> Read for PageReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        let mut data = buf;
        let mut read_bytes: usize = 0;

        while data.len() > 0 {
            let remaining_block_space = BLOCK_DATA_SIZE - self.block_offset;
            if data.len() <= remaining_block_space {
                self.copy_block(&mut data);
                return Ok(buf_len);
            }

            self.copy_block(&mut data[..remaining_block_space]);
            read_bytes += remaining_block_space;
            data = &mut data[remaining_block_space..];

            if !self.go_to_next_block()? {
                break;
            }
        }

        Ok(read_bytes)
    }
}

impl<'a> PageReader<'a> {
    pub fn new(page_manager: &'a mut PageManager, start_address: BlockAddress) -> Result<Self> {
        let page = page_manager.get_page(start_address.page_index)?;
        Ok(PageReader {
            page_manager,
            current_page: page,
            block_index: start_address.block_index,
            block_offset: 0
        })
    }

    pub fn skip(&mut self, skip: usize) -> Result<()> {
        let mut skip_mut = skip;
        loop {
            let remaining_block_space = BLOCK_DATA_SIZE - self.block_offset;
            if skip_mut <= remaining_block_space {
                self.block_offset += skip_mut;
                return Ok(());
            }

            if !self.go_to_next_block()? {
                return Err(Error::new(std::io::ErrorKind::Other, "Skip too big"));
            }

            skip_mut -= remaining_block_space;
        }
    }

    fn go_to_next_block(&mut self) -> Result<bool> {
        let next_block_address = get_next_block_address(&self.current_page, self.block_index);
        if next_block_address == BlockAddress::invalid() {
            return Ok(false);
        }

        if next_block_address.page_index != self.current_page.index() {
            self.current_page = self.page_manager.get_page(next_block_address.page_index)?;
        }

        self.block_index = next_block_address.block_index;
        self.block_offset = 0;

        Ok(true)
    }

    fn copy_block(&mut self, buffer: &mut [u8]) {
        let data_ref = self.current_page.get_block_data(self.block_index, self.block_offset,
             buffer.len());
        buffer.copy_from_slice(data_ref.as_ref());
        self.block_offset += buffer.len();
    }
}

pub struct PageWriter<'a> {
    page_manager: &'a mut PageManager,
    current_page: PageAccessor,
    block_address: BlockAddress,
    block_offset: usize,
    start_address: BlockAddress,
}

impl<'a> Write for PageWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut data = buf;
        loop {
            let remaining_block_space = BLOCK_DATA_SIZE - self.block_offset;
            if data.len() <= remaining_block_space {
                self.copy_to_block(data);
                return Ok(buf.len());
            }

            self.copy_to_block(&buf[..remaining_block_space]);
            self.go_to_next_block()?;

            data = &data[remaining_block_space..];
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_final_block();
        Ok(())
    }
}

impl<'a> PageWriter<'a> {
    pub fn new(page_manager: &'a mut PageManager) -> Result<Self> {
        let page = page_manager.get_page_with_free_blocks(0)?;
        let start_address = BlockAddress::new(page.index(), page.first_free_block());
        Ok(PageWriter {
            page_manager,
            current_page: page,
            block_address: start_address,
            start_address,
            block_offset: 0,
        })
    }

    pub fn start_address(&self) -> BlockAddress {
        self.start_address
    }

    fn copy_to_block(&mut self, buf: &[u8]) {
        self.current_page.set_block_data(self.block_address.block_index, buf, self.block_offset);
        self.block_offset += buf.len();
    }

    fn go_to_next_block(&mut self) -> Result<()> {
        self.block_offset = 0;

        if !self.current_page.has_free_blocks() {
            self.current_page = self.page_manager.get_page_with_free_blocks(self.current_page.index() + 1)?;
        }

        let current_page = &mut self.current_page;

        let prev_block_address = self.block_address;
        self.block_address = BlockAddress::new(current_page.index(), current_page.first_free_block());

        set_next_block_address(current_page, self.block_address.block_index, BlockAddress::invalid());
        if prev_block_address != BlockAddress::invalid() {
            let BlockAddress { page_index: prev_page_index, block_index: prev_block_index } = prev_block_address;
            if prev_page_index == current_page.index() {
                set_next_block_address(current_page, prev_block_index, self.block_address);
            }
            else {
                set_next_block_address(&mut self.page_manager.get_page(prev_page_index)?, prev_block_index, self.block_address);
            }
        }

        if self.start_address == BlockAddress::invalid() {
            self.start_address = self.block_address;
        }

        Ok(())
    }

    fn flush_final_block(&mut self) {
        set_next_block_address(&mut self.current_page, self.block_address.block_index, BlockAddress::invalid());
    }
}

impl<'a> Drop for PageWriter<'a> {
    fn drop(&mut self) {
        self.flush_final_block();
    }
}

fn set_next_block_address(page: &mut PageAccessor, block_index: u8, next_block_address: BlockAddress) {
    let mut buffer = [0; BlockAddress::size_in_buffer()];
    buffer.write_structure(&next_block_address);
    page.set_block_data(block_index, &buffer, BLOCK_DATA_SIZE);
}

fn get_next_block_address(page: &PageAccessor, block_index: u8) -> BlockAddress {
    page
        .get_block_data(block_index, BLOCK_DATA_SIZE, BlockAddress::size_in_buffer())
        .read_structure()
}