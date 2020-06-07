use std::borrow::Cow;
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::FileExt;

use thiserror::Error;

const PAGE_SIZE: usize = 4096;

const BACKING_PAGE_MARKER: u8 = 1;

pub trait PageModificationGuard {
    fn commit(self) -> TupleID;
    fn rollback(self);
}

pub trait SegmentController {
    fn read_segments_into(&mut self, start_segment_id: usize, bytes: &mut [u8]) -> Result<()>;

    fn read_segment(&mut self, segment_id: usize) -> Result<[u8; PAGE_SIZE]> {
        let mut bytes = [0; PAGE_SIZE];
        self.read_segments_into(segment_id, &mut bytes)?;
        Ok(bytes)
    }

    fn write_segments(&mut self, start_segment_id: usize, bytes: &[u8]) -> Result<()>;
}

pub trait SegmentControllerEx: SegmentController {
    fn read_segment_into_ex(&self, segment_id: usize, bytes: &mut [u8; PAGE_SIZE]) -> Result<()>;

    fn read_segment_ex(&self, segment_id: usize) -> Result<[u8; PAGE_SIZE]> {
        let mut bytes = [0; PAGE_SIZE];
        self.read_segment_into_ex(segment_id, &mut bytes)?;
        Ok(bytes)
    }

    fn write_segment_ex(&self, segment_id: usize, bytes: &[u8; PAGE_SIZE]) -> Result<()>;
}

pub enum Page {
    Simple(BackingPage),
    Extended(ExtendedPage),
}

impl Page {
    fn new_from_item_size(page_id: usize, size: usize) -> Self {
        if size + 7 > PAGE_SIZE {
            Page::Extended(ExtendedPage::empty_page(page_id, size))
        } else {
            Page::Simple(BackingPage::empty_page(page_id))
        }
    }

    fn from_bytes(page_id: usize, bytes: [u8; PAGE_SIZE]) -> Result<Page> {
        match bytes[0] {
            BACKING_PAGE_MARKER => Ok(Page::Simple(BackingPage { page_id, bytes })),
            x => Err(SlottedPageError::UnknownPageType(x)),
        }
    }

    fn to_bytes(&self) -> Cow<[u8]> {
        match self {
            Page::Simple(page) => page.to_bytes(),
            Page::Extended(page) => page.to_bytes(),
        }
    }

    fn page_id(&self) -> usize {
        match self {
            Page::Simple(page) => page.page_id,
            Page::Extended(page) => page.page_id(),
        }
    }

    fn get_entry_byte_range(&self, entry: usize) -> Option<&[u8]> {
        match self {
            Page::Simple(page) => page.get_entry_byte_range(entry),
            Page::Extended(page) => page.get_entry_byte_range(entry),
        }
    }

    fn free_space_left(&self) -> usize {
        match self {
            Page::Simple(page) => page.free_space_left() as usize,
            Page::Extended(page) => page.free_space_left() as usize,
        }
    }

    fn reserve_space(&mut self, length: usize) -> Option<BackingPageGuard> {
        match self {
            Page::Simple(page) => page.reserve_space(length as u16),
            Page::Extended(page) => unimplemented!(),
        }
    }
}

#[derive(Error, Debug)]
pub enum SlottedPageError {
    #[error("Malformed File Header")]
    MalformedFileHeader,

    #[error("Unknown page type")]
    UnknownPageType(u8),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("Entry `{}` not found on page `{}`", .0.slot, .0.page)]
    NotFound(TupleID),
}

type Result<T> = std::result::Result<T, SlottedPageError>;

impl SegmentController for File {
    fn read_segments_into(&mut self, segment_id: usize, bytes: &mut [u8]) -> Result<()> {
        let length = (bytes.len() / PAGE_SIZE) * PAGE_SIZE;
        let bytes = &mut bytes[..length];
        self.read_exact_at(bytes, (segment_id * PAGE_SIZE) as u64)?;
        Ok(())
    }

    fn write_segments(&mut self, start_segment_id: usize, bytes: &[u8]) -> Result<()> {
        let length = (bytes.len() / PAGE_SIZE) * PAGE_SIZE;
        let bytes = &bytes[..length];
        self.write_all_at(bytes, (start_segment_id * PAGE_SIZE) as u64)
            .map_err(SlottedPageError::from)
    }
}

#[cfg(unix)]
impl SegmentControllerEx for File {
    fn read_segment_into_ex(&self, segment_id: usize, bytes: &mut [u8; PAGE_SIZE]) -> Result<()> {
        self.read_exact_at(bytes, (segment_id * PAGE_SIZE) as u64)?;
        Ok(())
    }

    fn write_segment_ex(&self, segment_id: usize, bytes: &[u8; PAGE_SIZE]) -> Result<()> {
        self.write_all_at(bytes, (segment_id * PAGE_SIZE) as u64)
            .map_err(SlottedPageError::from)
    }
}

#[derive(Debug)]
pub struct TupleID {
    pub page: usize,
    pub slot: usize,
}

impl TupleID {
    fn with_page_and_slot(page: usize, slot: usize) -> Self {
        TupleID { page, slot }
    }
}

pub struct FileHeader {
    tuples: usize,
    pages: usize,
}

impl FileHeader {
    pub fn new() -> Self {
        FileHeader {
            tuples: 0,
            pages: 0,
        }
    }

    pub fn from_bytes(bytes: [u8; PAGE_SIZE]) -> Result<Self> {
        if bytes.starts_with(b"MAGIC") {
            let tuples = usize::from_ne_bytes([
                bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
            ]);
            let pages = usize::from_ne_bytes([
                bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
                bytes[20],
            ]);
            Ok(FileHeader { tuples, pages })
        } else {
            Err(SlottedPageError::MalformedFileHeader)
        }
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0u8; PAGE_SIZE];
        bytes[0..5].copy_from_slice(b"MAGIC");
        bytes[5..13].copy_from_slice(&self.tuples.to_ne_bytes());
        bytes[13..21].copy_from_slice(&self.pages.to_ne_bytes());
        bytes
    }
}

// First two bytes indicate the offset of the end of the pointer section of the page.
// The third and fourth bytes indicate the start of the used data section of the page.
pub struct BackingPage {
    page_id: usize,
    bytes: [u8; PAGE_SIZE],
}

impl std::fmt::Debug for BackingPage {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("BackingPage")
            .field("page_id", &self.page_id)
            .finish()
    }
}

impl BackingPage {
    pub fn from_bytes(page_id: usize, bytes: [u8; PAGE_SIZE]) -> Result<Page> {
        match bytes[0] {
            BACKING_PAGE_MARKER => Ok(Page::Simple(BackingPage { page_id, bytes })),
            x => Err(SlottedPageError::UnknownPageType(x)),
        }
    }

    fn to_bytes(&self) -> Cow<[u8]> {
        Cow::Borrowed(&self.bytes)
    }

    fn empty_page(page_id: usize) -> Self {
        let mut bytes: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        bytes[0] = BACKING_PAGE_MARKER;
        bytes[1..3].copy_from_slice(&5u16.to_ne_bytes());
        bytes[3..5].copy_from_slice(&(PAGE_SIZE as u16).to_ne_bytes());
        BackingPage { page_id, bytes }
    }

    fn entry_pointer_end(&self) -> u16 {
        u16::from_ne_bytes([self.bytes[1], self.bytes[2]])
    }

    fn set_entry_pointer_end(&mut self, entry_pointer_end: u16) {
        self.bytes[1..3].copy_from_slice(&entry_pointer_end.to_ne_bytes());
    }
    fn used_space_start(&self) -> u16 {
        u16::from_ne_bytes([self.bytes[3], self.bytes[4]])
    }

    fn set_used_space_start(&mut self, used_space_start: u16) {
        self.bytes[3..5].copy_from_slice(&used_space_start.to_ne_bytes());
    }

    fn free_space_slice(&self) -> &[u8] {
        &self.bytes[self.entry_pointer_end() as usize..self.used_space_start() as usize]
    }

    fn num_entries(&self) -> usize {
        (self.entry_pointer_end() as usize - 5) / 2
    }

    fn free_space_left(&self) -> u32 {
        self.free_space_slice().len() as u32 - 2
    }

    fn get_entry_offset_and_length(&self, entry: usize) -> Option<(u16, u16)> {
        if entry < self.num_entries() {
            let packed_offset_and_length: u16 =
                u16::from_ne_bytes([self.bytes[5 + 2 * entry], self.bytes[5 + 2 * entry + 1]]);
            let (mut offset, mut length) =
                { (packed_offset_and_length >> 4, packed_offset_and_length & 15) };
            if length == 15 {
                length = u16::from_ne_bytes([
                    self.bytes[offset as usize],
                    self.bytes[offset as usize + 1],
                ]);
                offset += 2;
            }
            Some((offset, length))
        } else {
            None
        }
    }

    fn get_entry_byte_range(&self, entry: usize) -> Option<&[u8]> {
        self.get_entry_offset_and_length(entry)
            .map(|(o, l)| &self.bytes[o as usize..(o + l) as usize])
    }

    fn reserve_space(&mut self, length: u16) -> Option<BackingPageGuard> {
        let (_, remaining_bytes) = self.bytes.split_at_mut(1);
        let (entry_pointer_end_bytes, remaining_bytes) = remaining_bytes.split_at_mut(2);
        let (used_space_start_bytes, mut remaining_bytes) = remaining_bytes.split_at_mut(2);

        let entry_pointer_end =
            u16::from_ne_bytes([entry_pointer_end_bytes[0], entry_pointer_end_bytes[1]]);
        let used_space_start =
            u16::from_ne_bytes([used_space_start_bytes[0], used_space_start_bytes[1]]);
        remaining_bytes = &mut remaining_bytes[entry_pointer_end as usize - 5..];
        let available_space = used_space_start - entry_pointer_end;
        let data_required_space = if length >= 15 { 2 + length } else { length };
        let total_required_space = data_required_space + 2;

        if available_space >= total_required_space {
            let newly_used_bytes_offset = used_space_start - data_required_space;
            let (free_bytes, mut newly_used_bytes) = remaining_bytes
                .split_at_mut((newly_used_bytes_offset - entry_pointer_end) as usize);
            let offset = newly_used_bytes_offset << 4 | (length & 15);
            let offset_bytes = offset.to_ne_bytes();
            free_bytes[0] = offset_bytes[0];
            free_bytes[1] = offset_bytes[1];

            if length >= 15 {
                let length_bytes = length.to_ne_bytes();
                newly_used_bytes[0] = length_bytes[0];
                newly_used_bytes[1] = length_bytes[1];
                newly_used_bytes = &mut newly_used_bytes[2..];
            }
            Some(BackingPageGuard {
                tuple: TupleID::with_page_and_slot(
                    self.page_id,
                    (entry_pointer_end as usize - 4) / 2,
                ),
                data_bytes: &mut newly_used_bytes[..length as usize],
                entry_pointer_end_bytes,
                used_space_start_bytes,
                new_entry_pointer_end: entry_pointer_end + 2,
                new_used_space_start: newly_used_bytes_offset,
            })
        } else {
            None
        }
    }
}

impl Deref for BackingPage {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }
}

impl DerefMut for BackingPage {
    fn deref_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }
}

#[derive(Debug)]
pub struct BackingPageGuard<'a> {
    tuple: TupleID,
    data_bytes: &'a mut [u8],
    new_entry_pointer_end: u16,
    entry_pointer_end_bytes: &'a mut [u8],
    new_used_space_start: u16,
    used_space_start_bytes: &'a mut [u8],
}

impl<'a> PageModificationGuard for BackingPageGuard<'a> {
    fn rollback(self) {}

    fn commit(self) -> TupleID {
        let new_entry_pointer_end_bytes = self.new_entry_pointer_end.to_ne_bytes();
        self.entry_pointer_end_bytes[0] = new_entry_pointer_end_bytes[0];
        self.entry_pointer_end_bytes[1] = new_entry_pointer_end_bytes[1];
        let new_used_space_start_bytes = self.new_used_space_start.to_ne_bytes();
        self.used_space_start_bytes[0] = new_used_space_start_bytes[0];
        self.used_space_start_bytes[1] = new_used_space_start_bytes[1];
        self.tuple
    }
}

impl<'a> Deref for BackingPageGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data_bytes
    }
}

impl<'a> DerefMut for BackingPageGuard<'a> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data_bytes
    }
}

// An extended page holds only a single item. The length of this item is stored in the first four
// bytes. The number of pages that compose the the extended page is calculated as:
// (length + 4) / PAGE_SIZE rounded up.
// Each entry thing is 24 bits:
// 012345678901234567890123
// ppppppppppppssssssssxxxx
// Position is 12 bytes for exact positioning
// Size is 8 bits: 255 means the size that is kept in the first 2 bytes of the payload
// Reference counter is 4 bits: 15 means the number is kept in the first 8 bytes of the payload
pub struct ExtendedPage {
    page_id: usize,
    bytes: Vec<u8>,
}

impl ExtendedPage {
    fn empty_page(page_id: usize, size: usize) -> Self {
        let mut bytes = Vec::new();
        let mut entry_bytes = if size >= 7 {
            size + 4 // 4 for the size and 4 for the number of pointers
        } else {
            size + 4 // 4 for the size
        };
        let required_length = size + 7;
        let full_pages = required_length / PAGE_SIZE;
        let initial_bytes = required_length % PAGE_SIZE;
        bytes.resize_with(((size + 7) / PAGE_SIZE) * PAGE_SIZE, Default::default);
        bytes[0] = BACKING_PAGE_MARKER;
        bytes[1..3].copy_from_slice(&((size + 7) / PAGE_SIZE).to_ne_bytes()); // Initial full pages
        bytes[3..5].copy_from_slice(&7u16.to_ne_bytes()); // End of the entry bytes - initially 7
        bytes[5..7].copy_from_slice(&(PAGE_SIZE as u16).to_ne_bytes()); // The
        ExtendedPage { page_id, bytes }
    }

    fn bytes_len(&self) -> usize {
        self.bytes.len()
    }

    fn data_len(&self) -> usize {
        let bytes = &self.bytes;
        usize::from_ne_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ])
    }

    fn pages_len(&self) -> usize {
        self.bytes_len() / PAGE_SIZE
    }

    fn page_id(&self) -> usize {
        self.page_id
    }

    fn to_bytes(&self) -> Cow<[u8]> {
        Cow::Borrowed(&self.bytes)
    }

    fn entry_pointer_end(&self) -> u16 {
        u16::from_ne_bytes([self.bytes[1], self.bytes[2]])
    }

    fn num_entries(&self) -> usize {
        (self.entry_pointer_end() as usize - 5) / 2
    }

    fn get_entry_offset_and_length(&self, entry: usize) -> Option<(usize, usize)> {
        if entry < self.num_entries() {
            let packed_offset_and_length: u16 =
                u16::from_ne_bytes([self.bytes[5 + 2 * entry], self.bytes[5 + 2 * entry + 1]]);
            let (mut offset, mut length) = {
                (
                    (packed_offset_and_length >> 4) as usize,
                    (packed_offset_and_length & 15) as usize,
                )
            };
            if length == 15 {
                length = usize::from_ne_bytes([
                    self.bytes[offset],
                    self.bytes[offset + 1],
                    self.bytes[offset + 2],
                    self.bytes[offset + 3],
                    self.bytes[offset + 4],
                    self.bytes[offset + 5],
                    self.bytes[offset + 6],
                    self.bytes[offset + 7],
                ]);
                offset += 8;
            }
            Some((offset, length))
        } else {
            None
        }
    }

    fn get_entry_byte_range(&self, entry: usize) -> Option<&[u8]> {
        self.get_entry_offset_and_length(entry)
            .map(|(o, l)| &self.bytes[o as usize..(o + l) as usize])
    }

    fn used_space_start(&self) -> u16 {
        u16::from_ne_bytes([self.bytes[3], self.bytes[4]])
    }

    fn free_space_slice(&self) -> &[u8] {
        &self.bytes[self.entry_pointer_end() as usize..self.used_space_start() as usize]
    }

    fn free_space_left(&self) -> u32 {
        self.free_space_slice().len() as u32 - 2
    }

    fn reserve_space(&mut self, length: u16) -> Option<BackingPageGuard> {
        let (_, remaining_bytes) = self.bytes.split_at_mut(1);

        let (entry_pointer_end_bytes, remaining_bytes) = remaining_bytes.split_at_mut(2);
        let (used_space_start_bytes, mut remaining_bytes) = remaining_bytes.split_at_mut(2);

        let entry_pointer_end =
            u16::from_ne_bytes([entry_pointer_end_bytes[0], entry_pointer_end_bytes[1]]);
        let used_space_start =
            u16::from_ne_bytes([used_space_start_bytes[0], used_space_start_bytes[1]]);
        remaining_bytes = &mut remaining_bytes[entry_pointer_end as usize - 5..];
        let available_space = used_space_start - entry_pointer_end;
        let data_required_space = if length >= 15 { 2 + length } else { length };
        let total_required_space = data_required_space + 2;

        if available_space >= total_required_space {
            let newly_used_bytes_offset = used_space_start - data_required_space;
            let (free_bytes, mut newly_used_bytes) = remaining_bytes
                .split_at_mut((newly_used_bytes_offset - entry_pointer_end) as usize);
            let offset = newly_used_bytes_offset << 4 | (length & 15);
            let offset_bytes = offset.to_ne_bytes();
            free_bytes[0] = offset_bytes[0];
            free_bytes[1] = offset_bytes[1];

            if length >= 15 {
                let length_bytes = length.to_ne_bytes();
                newly_used_bytes[0] = length_bytes[0];
                newly_used_bytes[1] = length_bytes[1];
                newly_used_bytes = &mut newly_used_bytes[2..];
            }
            Some(BackingPageGuard {
                tuple: TupleID::with_page_and_slot(
                    self.page_id,
                    (entry_pointer_end as usize - 4) / 2,
                ),
                data_bytes: &mut newly_used_bytes[..length as usize],
                entry_pointer_end_bytes,
                used_space_start_bytes,
                new_entry_pointer_end: entry_pointer_end + 2,
                new_used_space_start: newly_used_bytes_offset,
            })
        } else {
            None
        }
    }
}

pub trait PageController<'a> {
    type PageGuard: PageModificationGuard + DerefMut<Target = [u8]>;

    fn get_header(&self) -> &FileHeader;

    fn get_entry_bytes(&mut self, tuple: TupleID) -> Result<&[u8]>;

    fn reserve_space(&'a mut self, length: usize) -> Result<Self::PageGuard>;
}

struct NaivePageController<S: SegmentController> {
    header: FileHeader,
    header_is_dirty: bool,
    segment_controller: S,
    current_page: Option<(bool, Page)>,
}

impl<S: SegmentController> NaivePageController<S> {
    pub fn from_new(segment_controller: S) -> Result<Self> {
        let header = FileHeader::new();
        Ok(NaivePageController {
            header,
            header_is_dirty: true,
            segment_controller,
            current_page: None,
        })
    }

    pub fn from_existing(mut segment_controller: S) -> Result<Self> {
        let header = FileHeader::from_bytes(segment_controller.read_segment(0)?)?;
        Ok(NaivePageController {
            header,
            header_is_dirty: false,
            segment_controller,
            current_page: None,
        })
    }

    fn load_page(&mut self, page_id: usize) -> Result<()> {
        if let Some((_, old_page)) = &self.current_page {
            if old_page.page_id() != page_id {
                self.save_current_page(false)?;
                let bytes = self.segment_controller.read_segment(page_id)?;
                self.current_page = Some((false, Page::from_bytes(page_id, bytes)?));
            }
        } else {
            let bytes = self.segment_controller.read_segment(page_id)?;
            self.current_page = Some((false, Page::from_bytes(page_id, bytes)?));
        }
        Ok(())
    }

    fn save_current_page(&mut self, force: bool) -> Result<()> {
        if let Some((dirty, old_page)) = &mut self.current_page {
            if *dirty || force {
                self.segment_controller
                    .write_segments(old_page.page_id(), old_page.to_bytes().as_ref())?;
                *dirty = false;
            }
        }
        Ok(())
    }
}

impl<'a, S: SegmentController> PageController<'a> for NaivePageController<S> {
    type PageGuard = BackingPageGuard<'a>;

    fn get_header(&self) -> &FileHeader {
        &self.header
    }

    fn get_entry_bytes(&mut self, tuple: TupleID) -> Result<&[u8]> {
        self.load_page(tuple.page)?;
        let page = self.current_page.as_ref().unwrap();
        page.1
            .get_entry_byte_range(tuple.slot)
            .ok_or_else(|| SlottedPageError::NotFound(tuple))
    }

    fn reserve_space(&'a mut self, length: usize) -> Result<Self::PageGuard> {
        if let Some((dirty, page)) = &mut self.current_page {
            *dirty = true;
            if page.free_space_left() < length {
                self.header.pages += 1;
                self.header_is_dirty = true;
                self.current_page = Some((
                    true,
                    Page::new_from_item_size(self.header.pages as usize, length),
                ));
            }
        } else {
            self.header.pages += 1;
            self.header_is_dirty = true;
            self.current_page = Some((
                true,
                Page::new_from_item_size(self.header.pages as usize, length),
            ));
        }
        Ok(self
            .current_page
            .as_mut()
            .unwrap()
            .1
            .reserve_space(length)
            .unwrap())
    }
}

impl<S: SegmentController> Drop for NaivePageController<S> {
    fn drop(&mut self) {
        if self.header_is_dirty {
            self.segment_controller
                .write_segments(0, &self.header.to_bytes())
                .ok();
        }
        self.save_current_page(false).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn single_writer() {
        let mut file = NaivePageController::from_new(File::create("test.lol").unwrap()).unwrap();
        let mut guard = file.reserve_space(8).unwrap();
        (&mut *guard).copy_from_slice(&5usize.to_ne_bytes());
        guard.commit();
    }

    #[test]
    fn double_writer() {
        let mut file = NaivePageController::from_new(File::create("test_2.lol").unwrap()).unwrap();
        let mut guard = file.reserve_space(8).unwrap();
        (&mut *guard).copy_from_slice(&5usize.to_ne_bytes());
        guard.commit();
        let mut guard = file.reserve_space(8).unwrap();
        (&mut *guard).copy_from_slice(&900usize.to_ne_bytes());
        guard.commit();
        let mut guard = file.reserve_space(6).unwrap();
        (&mut *guard).copy_from_slice("roflpi".as_bytes());
        guard.commit();
    }

    #[test]
    fn single_reader() {
        let file = File::open("test.lol").unwrap();
        let mut file = NaivePageController::from_existing(file).unwrap();
        let bytes = file
            .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
            .unwrap();
        let entry = usize::from_ne_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        assert_eq!(entry, 5);
    }

    #[test]
    fn double_reader() {
        let file = File::open("test_2.lol").unwrap();
        let mut file = NaivePageController::from_existing(file).unwrap();
        let bytes = file
            .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
            .unwrap();
        let entry = usize::from_ne_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        assert_eq!(entry, 5);
        let bytes = file
            .get_entry_bytes(TupleID::with_page_and_slot(1, 1))
            .unwrap();
        let entry = usize::from_ne_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        assert_eq!(entry, 900);
        let bytes = file
            .get_entry_bytes(TupleID::with_page_and_slot(1, 2))
            .unwrap();
        let entry = std::str::from_utf8(bytes).unwrap();
        assert_eq!(entry, "roflpi");
    }
}
