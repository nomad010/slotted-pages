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
    fn new_from_item_size(page_id: usize, references: usize, size: usize) -> Self {
        Page::Extended(ExtendedPage::empty_page(page_id, references, size))
    }

    fn from_bytes(page_id: usize, bytes: Vec<u8>) -> Result<Page> {
        match bytes[0] {
            BACKING_PAGE_MARKER => Ok(Page::Extended(ExtendedPage { page_id, bytes })),
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
        // println!("roflpi {}", entry);
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

    fn reserve_space(&mut self, references: usize, length: usize) -> Option<BackingPageGuard> {
        match self {
            Page::Simple(page) => page.reserve_space(length as u16),
            Page::Extended(page) => page.reserve_space(references, length),
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
        println!("{}", segment_id);
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
        u16::from_ne_bytes([self.bytes[3], self.bytes[4]])
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
        // println!("gar {}", self.entry_pointer_end());
        (self.entry_pointer_end() as usize - 7) / 2
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
        // println!("Simple get_entry_byte_range({})", entry);
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
    fn additional_segments(bytes: &[u8]) -> usize {
        u16::from_ne_bytes([bytes[1], bytes[2]]) as usize
    }

    fn from_bytes<S>(page_id: usize, controller: &mut S) -> Result<Self>
    where
        S: SegmentController,
    {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.resize_with(PAGE_SIZE, Default::default);
        controller.read_segments_into(page_id, &mut bytes)?;
        let additional_segments = Self::additional_segments(&bytes);
        println!("derp load page {}", additional_segments);
        bytes.resize_with(PAGE_SIZE * (additional_segments + 1), Default::default);
        controller.read_segments_into(page_id + 1, &mut bytes[PAGE_SIZE..])?;
        Ok(ExtendedPage { page_id, bytes })
    }

    pub fn total_required_size(size: usize, references: usize) -> (usize, bool, bool) {
        let mut size = size;
        let mut has_extra_size = false;
        if size >= 255 {
            has_extra_size = true;
            size += 2;
        }
        let mut has_extra_references = false;
        if references >= 15 {
            has_extra_references = true;
            size += 8;
        }
        (size, has_extra_size, has_extra_references)
    }

    pub fn to_entry(position: usize, size: usize, references: usize) -> ([u8; 3], bool, bool) {
        assert!(position < PAGE_SIZE);
        assert!(size < PAGE_SIZE);

        let mut bytes = [0u8, 0u8, 0u8];
        bytes[0..2].copy_from_slice(&((position << 4) as u16).to_ne_bytes());
        let has_extra_references = if references >= 15 {
            bytes[1] |= 15;
            true
        } else {
            bytes[1] |= references as u8;
            false
        };
        let has_extra_size = if size >= 255 {
            bytes[2] = 255;
            true
        } else {
            bytes[2] = size as u8;
            false
        };
        (bytes, has_extra_size, has_extra_references)
    }

    fn from_entry(bytes: [u8; 3]) -> (usize, usize, usize) {
        let position_and_references = u16::from_ne_bytes([bytes[0], bytes[1]]) as usize;
        let position = position_and_references >> 4;
        let references = position_and_references & 15;
        let size = u8::from_ne_bytes([bytes[2]]) as usize;
        // println!(
        //     "derp {} {} {} {} {:?}",
        //     position, size, references, position_and_references, bytes
        // );
        (position, size, references)
    }

    fn empty_page(page_id: usize, references: usize, size: usize) -> Self {
        let bytes: Vec<u8> = Vec::new();
        ExtendedPage { page_id, bytes }
    }

    fn resize(&mut self, references: usize, size: usize) {
        let total_data_size = Self::total_required_size(size, references).0;
        let required_length = total_data_size + 10;
        let total_pages = (required_length + PAGE_SIZE - 1) / PAGE_SIZE;
        let total_size = total_pages * PAGE_SIZE;
        self.bytes.resize_with(total_size, Default::default);

        self.bytes[0] = BACKING_PAGE_MARKER;
        self.bytes[1..3].copy_from_slice(&(total_pages as u16 - 1).to_ne_bytes()); // Initial full pages
        self.bytes[3..5].copy_from_slice(&7u16.to_ne_bytes()); // End of the entry bytes - initial 7
        self.bytes[5..7].copy_from_slice(&(PAGE_SIZE as u16).to_ne_bytes()); // Start of data bytes
    }

    fn bytes_len(&self) -> usize {
        self.bytes.len()
    }

    fn extra_segments(&self) -> usize {
        u16::from_ne_bytes([self.bytes[1], self.bytes[2]]) as usize
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
        u16::from_ne_bytes([self.bytes[3], self.bytes[4]])
    }

    fn num_entries(&self) -> usize {
        (self.entry_pointer_end() as usize - 7) / 3
    }

    fn unpack_entry(&self, entry: usize) -> Option<(usize, usize, usize)> {
        // println!("rofl {} {}", entry, self.num_entries());
        if entry < self.num_entries() {
            let entry_start = 7 + 3 * entry;
            let packed_entry = [
                self.bytes[entry_start],
                self.bytes[entry_start + 1],
                self.bytes[entry_start + 2],
            ];
            let (mut offset, mut size, mut references) = Self::from_entry(packed_entry);
            println!(
                "gargablegar {} {} {} {:#?}",
                offset, // Offset is getting written wrong
                size,
                references,
                &self.bytes[entry_start..entry_start + 3]
            );
            if size == 255 {
                size = u16::from_ne_bytes([self.bytes[offset], self.bytes[offset + 1]]) as usize;
                offset += 2;
            }
            if entry == 0 {
                println!("derp here {}", self.extra_segments());
                size += PAGE_SIZE * self.extra_segments();
                if offset < 10 {
                    size -= PAGE_SIZE;
                    offset += PAGE_SIZE;
                }
            }

            if references == 15 {
                references = usize::from_ne_bytes([
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
            Some((offset, size, references))
        } else {
            None
        }
    }

    fn get_entry_byte_range(&self, entry: usize) -> Option<&[u8]> {
        println!(
            "get_entry_byte_range({}) = {:?}",
            entry,
            self.unpack_entry(entry)
        );

        self.unpack_entry(entry)
            .map(|(offset, size, _)| &self.bytes[offset..offset + size])
    }

    fn used_space_start(&self) -> u16 {
        u16::from_ne_bytes([self.bytes[5], self.bytes[6]])
    }

    fn free_space_slice(&self) -> &[u8] {
        &self.bytes[self.entry_pointer_end() as usize..self.used_space_start() as usize]
    }

    fn free_space_left(&self) -> u32 {
        self.free_space_slice().len().saturating_sub(3) as u32
    }

    fn reserve_space(&mut self, references: usize, length: usize) -> Option<BackingPageGuard> {
        if self.bytes.is_empty() || self.num_entries() == 0 {
            self.resize(references, length);
            let bytes_length = self.bytes.len();
            let (_, remaining_bytes) = self.bytes.split_at_mut(1); // Page byte marker
            let (_, remaining_bytes) = remaining_bytes.split_at_mut(2); // Extra pages
            let (entry_pointer_end_bytes, remaining_bytes) = remaining_bytes.split_at_mut(2);
            let (used_space_start_bytes, remaining_bytes) = remaining_bytes.split_at_mut(2);

            let (total_data_size, has_extra_size, has_extra_references) =
                Self::total_required_size(length, references);
            let position = bytes_length - total_data_size;
            let written_size = length % PAGE_SIZE;
            println!(
                "{} {} {} {} {} {} {:?}",
                position,
                written_size,
                references,
                has_extra_size,
                has_extra_references,
                total_data_size,
                Self::to_entry(position, written_size, references)
            );
            remaining_bytes[..3]
                .copy_from_slice(&Self::to_entry(position, written_size, references).0);

            let bytes_start = remaining_bytes.len() - total_data_size;
            let mut bytes = &mut remaining_bytes[bytes_start..];
            if has_extra_size {
                bytes[..2].copy_from_slice(&(written_size as u16).to_ne_bytes());
                bytes = &mut bytes[2..];
            }
            println!("rawr {} ", bytes.len());
            if has_extra_references {
                bytes[..8].copy_from_slice(&references.to_ne_bytes());
                bytes = &mut bytes[8..];
            }
            Some(BackingPageGuard {
                tuple: TupleID::with_page_and_slot(self.page_id, 0),
                data_bytes: bytes,
                entry_pointer_end_bytes,
                used_space_start_bytes,
                new_entry_pointer_end: 10,
                new_used_space_start: ((PAGE_SIZE - (length % PAGE_SIZE)) % PAGE_SIZE) as u16,
            })
        } else {
            let (_, remaining_bytes) = self.bytes.split_at_mut(1); // Page byte marker
            let (_, remaining_bytes) = remaining_bytes.split_at_mut(2); // Extra pages
            let (entry_pointer_end_bytes, remaining_bytes) = remaining_bytes.split_at_mut(2);
            let (used_space_start_bytes, mut remaining_bytes) = remaining_bytes.split_at_mut(2);

            let entry_pointer_end =
                u16::from_ne_bytes([entry_pointer_end_bytes[0], entry_pointer_end_bytes[1]])
                    as usize;
            let used_space_start =
                u16::from_ne_bytes([used_space_start_bytes[0], used_space_start_bytes[1]]) as usize;
            remaining_bytes = &mut remaining_bytes[entry_pointer_end as usize - 7..];
            let available_space = used_space_start - entry_pointer_end;
            let (data_required_space, has_extra_size, has_extra_references) =
                Self::total_required_size(length, references);
            let total_required_space = data_required_space + 3;

            if available_space >= total_required_space {
                let position = used_space_start - data_required_space;
                let (free_bytes, mut newly_used_bytes) =
                    remaining_bytes.split_at_mut((position - entry_pointer_end) as usize);
                free_bytes[..3].copy_from_slice(&Self::to_entry(position, length, references).0);
                // println!("Gar {} {:?}", length, &free_bytes[..3]);

                if has_extra_size {
                    newly_used_bytes[..2].copy_from_slice(&(length as u16).to_ne_bytes());
                    newly_used_bytes = &mut newly_used_bytes[2..];
                }
                if has_extra_references {
                    newly_used_bytes[..8].copy_from_slice(&references.to_ne_bytes());
                    newly_used_bytes = &mut newly_used_bytes[8..];
                }
                // println!("roflpi position {}", position);
                Some(BackingPageGuard {
                    tuple: TupleID::with_page_and_slot(
                        self.page_id,
                        (entry_pointer_end as usize - 7) / 3,
                    ),
                    data_bytes: &mut newly_used_bytes[..length],
                    entry_pointer_end_bytes,
                    used_space_start_bytes,
                    new_entry_pointer_end: entry_pointer_end as u16 + 3,
                    new_used_space_start: position as u16,
                })
            } else {
                None
            }
        }
    }
}

pub trait PageController<'a> {
    type PageGuard: PageModificationGuard + DerefMut<Target = [u8]>;

    fn get_header(&self) -> &FileHeader;

    fn get_entry_bytes(&mut self, tuple: TupleID) -> Result<&[u8]>;

    fn reserve_space(&'a mut self, references: usize, data_size: usize) -> Result<Self::PageGuard>;
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
                self.current_page = Some((
                    false,
                    Page::Extended(ExtendedPage::from_bytes(
                        page_id,
                        &mut self.segment_controller,
                    )?),
                ));
            }
        } else {
            self.current_page = Some((
                false,
                Page::Extended(ExtendedPage::from_bytes(
                    page_id,
                    &mut self.segment_controller,
                )?),
            ));
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

    fn reserve_space(
        &'a mut self,
        references: usize,
        data_length: usize,
    ) -> Result<Self::PageGuard> {
        let length = references * 4 + data_length;
        if let Some((dirty, page)) = &mut self.current_page {
            *dirty = true;
            if page.free_space_left() < length {
                self.header.pages += 1;
                self.header_is_dirty = true;
                self.current_page = Some((
                    true,
                    Page::new_from_item_size(self.header.pages as usize, references, data_length),
                ));
            }
        } else {
            self.header.pages += 1;
            self.header_is_dirty = true;
            self.current_page = Some((
                true,
                Page::new_from_item_size(self.header.pages as usize, references, data_length),
            ));
        }
        Ok(self
            .current_page
            .as_mut()
            .unwrap()
            .1
            .reserve_space(references, length)
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
        let mut guard = file.reserve_space(0, 8).unwrap();
        (&mut *guard).copy_from_slice(&5usize.to_ne_bytes());
        guard.commit();
    }

    #[test]
    fn double_writer() {
        let mut file = NaivePageController::from_new(File::create("test_2.lol").unwrap()).unwrap();
        let mut guard = file.reserve_space(0, 8).unwrap();
        (&mut *guard).copy_from_slice(&5usize.to_ne_bytes());
        guard.commit();
        let mut guard = file.reserve_space(0, 8).unwrap();
        (&mut *guard).copy_from_slice(&900usize.to_ne_bytes());
        guard.commit();
        let mut guard = file.reserve_space(0, 6).unwrap();
        (&mut *guard).copy_from_slice("roflpi".as_bytes());
        guard.commit();
    }

    #[test]
    fn large_writer() {
        let mut file = NaivePageController::from_new(File::create("test_3.lol").unwrap()).unwrap();
        let mut guard = file.reserve_space(0, 30000).unwrap();
        (&mut *guard).copy_from_slice("lol".repeat(10000).as_bytes());
        guard.commit();
    }

    #[test]
    fn single_reader() {
        let file = File::open("test.lol").unwrap();
        let mut file = NaivePageController::from_existing(file).unwrap();
        let bytes = file
            .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
            .unwrap();
        // println!(
        //     "derp final bytes: {:?}",
        //     &[bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],]
        // );
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

    #[test]
    fn large_reader() {
        let mut file =
            NaivePageController::from_existing(File::open("test_3.lol").unwrap()).unwrap();
        let bytes = file
            .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
            .unwrap();
        let entry = std::str::from_utf8(bytes).unwrap();
        println!("wat {}", bytes.len());
        assert_eq!(entry, "lol".repeat(10000));
    }
}
