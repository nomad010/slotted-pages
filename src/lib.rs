use std::borrow::Cow;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::slice::SliceIndex;

use thiserror::Error;

const SEGMENT_SIZE: usize = 4096;

/*
TODO: We should replace this marker with a checksum in the page.
*/
const BACKING_PAGE_MARKER: u8 = 1;

pub trait SegmentController {
    fn read_segments_into(&mut self, start_segment_id: usize, bytes: &mut [u8]) -> Result<()>;

    fn read_segment(&mut self, segment_id: usize) -> Result<[u8; SEGMENT_SIZE]> {
        let mut bytes = [0; SEGMENT_SIZE];
        self.read_segments_into(segment_id, &mut bytes)?;
        Ok(bytes)
    }

    fn write_segments(&mut self, start_segment_id: usize, bytes: &[u8]) -> Result<()>;
}

impl<T> SegmentController for T
where
    T: Write + Read + Seek,
{
    fn read_segments_into(&mut self, segment_id: usize, bytes: &mut [u8]) -> Result<()> {
        let offset = segment_id * SEGMENT_SIZE;
        let length = (bytes.len() / SEGMENT_SIZE) * SEGMENT_SIZE;
        let bytes = &mut bytes[..length];
        println!("{}", segment_id);
        self.seek(SeekFrom::Start(offset as u64))?;
        self.read_exact(bytes)?;
        Ok(())
    }

    fn write_segments(&mut self, start_segment_id: usize, bytes: &[u8]) -> Result<()> {
        let offset = start_segment_id * SEGMENT_SIZE;
        let length = (bytes.len() / SEGMENT_SIZE) * SEGMENT_SIZE;
        let bytes = &bytes[..length];
        self.seek(SeekFrom::Start(offset as u64))?;
        self.write_all(bytes).map_err(SlottedPageError::from)
    }
}

#[derive(Error, Debug)]
pub enum SlottedPageError {
    #[error("Malformed file header")]
    MalformedFileHeader,

    #[error("Unknown page type")]
    UnknownPageType(u8),

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("Entry `{}` not found on page `{}`", .0.slot, .0.page)]
    NotFound(TupleID),

    #[error("Insufficient space for request. `{0}` requested, but `{1}` available")]
    InsufficientSpace(usize, usize),
}

type Result<T> = std::result::Result<T, SlottedPageError>;

#[derive(Clone, Copy, Debug)]
pub struct TupleID {
    pub page: usize,
    pub slot: usize,
}

impl TupleID {
    const SERIALIZED_SIZE: usize = 10;

    fn with_page_and_slot(page: usize, slot: usize) -> Self {
        TupleID { page, slot }
    }

    pub fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut result = [0; Self::SERIALIZED_SIZE];
        result[..8].copy_from_slice(&(self.page as u64).to_le_bytes());
        result[8..].copy_from_slice(&(self.slot as u16).to_le_bytes());
        result
    }

    pub fn to_ne_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut result = [0; Self::SERIALIZED_SIZE];
        result[..8].copy_from_slice(&self.page.to_ne_bytes());
        result[8..].copy_from_slice(&(self.slot as u16).to_ne_bytes());
        result
    }

    pub fn to_be_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut result = [0; Self::SERIALIZED_SIZE];
        result[..8].copy_from_slice(&self.page.to_be_bytes());
        result[8..].copy_from_slice(&(self.slot as u16).to_be_bytes());
        result
    }

    pub fn from_le_bytes(bytes: [u8; Self::SERIALIZED_SIZE]) -> Self {
        TupleID::with_page_and_slot(
            u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]) as usize,
            u16::from_le_bytes([bytes[8], bytes[9]]) as usize,
        )
    }

    pub fn from_ne_bytes(bytes: [u8; Self::SERIALIZED_SIZE]) -> Self {
        TupleID::with_page_and_slot(
            u64::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]) as usize,
            u16::from_ne_bytes([bytes[8], bytes[9]]) as usize,
        )
    }

    pub fn from_be_bytes(bytes: [u8; Self::SERIALIZED_SIZE]) -> Self {
        TupleID::with_page_and_slot(
            u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]) as usize,
            u16::from_be_bytes([bytes[8], bytes[9]]) as usize,
        )
    }
}

pub struct FileHeader {
    pages: usize,
    pub root: Option<TupleID>,
}

impl FileHeader {
    pub fn new() -> Self {
        FileHeader {
            pages: 0,
            root: None,
        }
    }

    pub fn from_bytes(bytes: [u8; SEGMENT_SIZE]) -> Result<Self> {
        if bytes.starts_with(b"MAGIC") {
            let pages = u64::from_ne_bytes([
                bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
            ]) as usize;
            let root = if bytes[13] != 0 {
                Some(TupleID::from_le_bytes([
                    bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20],
                    bytes[21], bytes[22], bytes[23],
                ]))
            } else {
                None
            };
            Ok(FileHeader { pages, root })
        } else {
            Err(SlottedPageError::MalformedFileHeader)
        }
    }

    pub fn to_bytes(&self) -> [u8; SEGMENT_SIZE] {
        let mut bytes = [0u8; SEGMENT_SIZE];
        bytes[0..5].copy_from_slice(b"MAGIC");
        bytes[5..13].copy_from_slice(&(self.pages as u64).to_le_bytes());
        bytes
    }
}

#[derive(Debug)]
pub struct BackingPageGuard<'a> {
    tuple: TupleID,
    data_bytes: *mut u8,
    data_length: usize,
    references: usize,
    new_entry_pointer_end: u16,
    entry_pointer_end_bytes: &'a mut [u8],
    new_used_space_start: u16,
    used_space_start_bytes: &'a mut [u8],
}

impl<'a> BackingPageGuard<'a> {
    pub fn tuple_id(&self) -> TupleID {
        self.tuple
    }

    pub fn add_reference(&mut self, tuple_id: TupleID) -> Result<()> {
        // TODO: Add reference is overwriting the next value's initial bytes.
        let serialized_reference = tuple_id.to_le_bytes();
        let data_bytes =
            unsafe { std::slice::from_raw_parts_mut(self.data_bytes, self.data_length) };
        println!("raew {}", self.data_length);
        if let Some(new_references) = self.references.checked_sub(1) {
            self.references = new_references;
            self.data_length -= serialized_reference.len();
            let start_bytes = data_bytes.len() - serialized_reference.len();
            data_bytes[start_bytes..].copy_from_slice(&serialized_reference);
            Ok(())
        } else {
            Err(SlottedPageError::InsufficientSpace(
                serialized_reference.len(),
                self.data_length,
            ))
        }
    }

    pub fn reserve_space(&mut self, size: usize) -> Result<&mut [u8]> {
        let available = self.data_length - self.references * TupleID::SERIALIZED_SIZE;
        let data_bytes =
            unsafe { std::slice::from_raw_parts_mut(self.data_bytes, self.data_length) };
        if size <= available {
            self.data_bytes = unsafe { self.data_bytes.add(size) };
            self.data_length -= size;
            Ok(&mut data_bytes[..size])
        } else {
            Err(SlottedPageError::InsufficientSpace(size, available))
        }
    }

    pub fn remaining_data_bytes(&mut self) -> &mut [u8] {
        let available = self.data_length - self.references * TupleID::SERIALIZED_SIZE;
        let data_bytes = unsafe { std::slice::from_raw_parts_mut(self.data_bytes, available) };
        self.data_length -= available;
        &mut data_bytes[self.data_length..]
    }

    fn inner_commit(&mut self) {
        println!(
            "Updating entry {:?} to {}",
            u16::from_le_bytes([
                self.entry_pointer_end_bytes[0],
                self.entry_pointer_end_bytes[1]
            ]),
            self.new_entry_pointer_end
        );
        println!(
            "Updating used space {:?} to {}",
            u16::from_le_bytes([
                self.used_space_start_bytes[0],
                self.used_space_start_bytes[1]
            ]),
            self.new_used_space_start
        );
        self.entry_pointer_end_bytes
            .copy_from_slice(&self.new_entry_pointer_end.to_le_bytes());
        self.used_space_start_bytes
            .copy_from_slice(&self.new_used_space_start.to_le_bytes());
    }

    pub fn commit(self) -> TupleID {
        self.tuple
    }
}

impl<'a> Drop for BackingPageGuard<'a> {
    fn drop(&mut self) {
        self.inner_commit();
    }
}

pub struct ByteRange<'a> {
    data_bytes: &'a [u8],
    reference_bytes: &'a [u8],
}

impl<'a> ByteRange<'a> {
    pub fn from_bytes(bytes: &'a [u8], references: usize) -> Self {
        let reference_start = bytes.len() - TupleID::SERIALIZED_SIZE * references;
        let (data_bytes, reference_bytes) = bytes.split_at(reference_start);
        ByteRange {
            data_bytes,
            reference_bytes,
        }
    }

    pub fn reference(&self, index: usize) -> Option<TupleID> {
        let reference_end = self.reference_bytes.len() - index * TupleID::SERIALIZED_SIZE;
        let bytes = &self.reference_bytes[reference_end - TupleID::SERIALIZED_SIZE..reference_end];
        if bytes.len() == 10 {
            Some(TupleID::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                bytes[8], bytes[9],
            ]))
        } else {
            None
        }
    }

    pub fn data_byte_range<R>(&self, range: R) -> &[u8]
    where
        R: RangeBounds<usize> + SliceIndex<[u8], Output = [u8]>,
    {
        &self.data_bytes[range]
    }

    pub fn split_at(&mut self, reference_position: usize, data_position: usize) -> Self {
        let (new_reference_bytes, split_reference_bytes) =
            self.reference_bytes.split_at(reference_position);
        let (new_data_bytes, split_data_bytes) = self.data_bytes.split_at(data_position);
        self.reference_bytes = new_reference_bytes;
        self.data_bytes = new_data_bytes;
        Self {
            reference_bytes: split_reference_bytes,
            data_bytes: split_data_bytes,
        }
    }
}

// struct InternalEntry {
//     position: usize,
//     size: usize,
//     reference: usize,
// }

// impl InternalEntry {}

// An extended page holds only a single item. The length of this item is stored in the first four
// bytes. The number of pages that compose the the extended page is calculated as:
// (length + 10) / SEGMENT_SIZE rounded up.
// Each entry thing is 24 bits:
// 012345678901234567890123
// ppppppppppppssssssssxxxx
// Position is 12 bytes for exact positioning
// Size is 8 bits: 255 means the size that is kept in the first 2 bytes of the payload
// Reference counter is 4 bits: 15 means the number is kept in the first 8 bytes of the payload
pub struct Page {
    page_id: usize,
    bytes: Vec<u8>,
}

impl Page {
    fn from_bytes<S>(page_id: usize, controller: &mut S) -> Result<Self>
    where
        S: SegmentController,
    {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.resize_with(SEGMENT_SIZE, Default::default);
        controller.read_segments_into(page_id, &mut bytes)?;
        let additional_segments = u16::from_ne_bytes([bytes[1], bytes[2]]) as usize;
        println!("derp extra pages {}", additional_segments);
        bytes.resize_with(SEGMENT_SIZE * (additional_segments + 1), Default::default);
        controller.read_segments_into(page_id + 1, &mut bytes[SEGMENT_SIZE..])?;
        Ok(Page { page_id, bytes })
    }

    pub fn total_required_size(data: usize, references: usize) -> (usize, bool, bool) {
        let mut size = data + TupleID::SERIALIZED_SIZE * references;
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
        assert!(position < SEGMENT_SIZE);
        assert!(size < SEGMENT_SIZE);

        println!(
            "wat {} and {} wat {:?} ",
            position,
            position << 4,
            &((position << 4) as u16).to_le_bytes()
        );

        let mut bytes = [0u8, 0u8, 0u8];
        let mut position_and_references = (position << 4) as u16;

        let has_extra_references = if references >= 15 {
            position_and_references |= 15;
            true
        } else {
            position_and_references |= references as u16;
            false
        };
        bytes[..2].copy_from_slice(&position_and_references.to_le_bytes());
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
        let position_and_references = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
        let position = position_and_references >> 4;
        let references = position_and_references & 15;
        let size = u8::from_le_bytes([bytes[2]]) as usize;
        println!(
            "derp {} {} {} {} {:?}",
            position, size, references, position_and_references, bytes
        );
        (position, size, references)
    }

    fn empty(page_id: usize) -> Self {
        let bytes: Vec<u8> = Vec::new();
        Page { page_id, bytes }
    }

    fn resize(&mut self, references: usize, size: usize) {
        let total_data_size = Self::total_required_size(size, references).0;
        let required_length = total_data_size + 10;
        let total_pages = (required_length + SEGMENT_SIZE - 1) / SEGMENT_SIZE;
        let total_size = total_pages * SEGMENT_SIZE;
        self.bytes.resize_with(total_size, Default::default);

        self.bytes[0] = BACKING_PAGE_MARKER;
        self.bytes[1..3].copy_from_slice(&(total_pages as u16 - 1).to_le_bytes()); // Initial full pages
        self.bytes[3..5].copy_from_slice(&7u16.to_le_bytes()); // End of the entry bytes - initial 7
        self.bytes[5..7].copy_from_slice(&(SEGMENT_SIZE as u16).to_le_bytes()); // Start of data bytes
    }

    fn page_id(&self) -> usize {
        self.page_id
    }

    fn to_bytes(&self) -> Cow<[u8]> {
        Cow::Borrowed(&self.bytes)
    }

    pub fn entry_pointer_end(&self) -> usize {
        u16::from_ne_bytes([self.bytes[3], self.bytes[4]]) as usize
    }

    pub fn num_entries(&self) -> usize {
        println!("rawr? {}", self.entry_pointer_end());
        (self.entry_pointer_end() - 7) / 3
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
                offset,
                size,
                references,
                &self.bytes[entry_start..entry_start + 3]
            );
            if size == 255 {
                println!("Actually the size is {:?}", &self.bytes[offset..offset + 2]);
                size = u16::from_ne_bytes([self.bytes[offset], self.bytes[offset + 1]]) as usize;
                offset += 2;
            }
            if entry == 0 {
                println!(
                    "derp here adding {} {}",
                    size,
                    ((self.bytes.len() / SEGMENT_SIZE) - 1) * SEGMENT_SIZE
                );
                size += SEGMENT_SIZE * ((self.bytes.len() / SEGMENT_SIZE) - 1);
                if offset <= 10 {
                    size -= SEGMENT_SIZE;
                    offset += SEGMENT_SIZE;
                }
            }

            if references == 15 {
                references = u64::from_ne_bytes([
                    self.bytes[offset],
                    self.bytes[offset + 1],
                    self.bytes[offset + 2],
                    self.bytes[offset + 3],
                    self.bytes[offset + 4],
                    self.bytes[offset + 5],
                    self.bytes[offset + 6],
                    self.bytes[offset + 7],
                ]) as usize;
                offset += 8;
            }
            Some((offset, size, references))
        } else {
            None
        }
    }

    fn get_entry_byte_range(&self, entry: usize) -> Option<ByteRange> {
        println!(
            "get_entry_byte_range({}) = {:?}",
            entry,
            self.unpack_entry(entry)
        );

        self.unpack_entry(entry).map(|(offset, size, references)| {
            let bytes = &self.bytes[offset..offset + size + TupleID::SERIALIZED_SIZE * references];
            ByteRange::from_bytes(bytes, references)
        })
    }

    pub fn used_space_start(&self) -> usize {
        if self.num_entries() == 0 {
            self.bytes.len()
        } else {
            u16::from_ne_bytes([self.bytes[5], self.bytes[6]]) as usize
        }
    }

    fn free_space_slice(&self) -> &[u8] {
        &self.bytes[self.entry_pointer_end()..self.used_space_start()]
    }

    fn free_space_left(&self) -> usize {
        self.free_space_slice().len().saturating_sub(3)
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
            let written_size = length % SEGMENT_SIZE;
            println!(
                "first entry {} {} {} {} {} {} {:?}",
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
                bytes[..2].copy_from_slice(&(written_size as u16).to_le_bytes());
                bytes = &mut bytes[2..];
            }
            println!("rawr {} extra {}", bytes.len(), has_extra_references);
            if has_extra_references {
                bytes[..8].copy_from_slice(&(references as u64).to_le_bytes());
                bytes = &mut bytes[8..];
            }
            println!(
                "gargable gar {} {}",
                total_data_size,
                ((SEGMENT_SIZE - (total_data_size % SEGMENT_SIZE)) % SEGMENT_SIZE)
            );
            Some(BackingPageGuard {
                tuple: TupleID::with_page_and_slot(self.page_id, 0),
                data_length: bytes.len(),
                data_bytes: bytes.as_mut_ptr(),
                entry_pointer_end_bytes,
                used_space_start_bytes,
                new_entry_pointer_end: 10,
                new_used_space_start: ((SEGMENT_SIZE - (total_data_size % SEGMENT_SIZE))
                    % SEGMENT_SIZE) as u16,
                references,
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
            println!("totreqsize: {}", data_required_space);
            let total_required_space = data_required_space + 3;

            if available_space >= total_required_space {
                let position = used_space_start - data_required_space;
                let (free_bytes, mut newly_used_bytes) =
                    remaining_bytes.split_at_mut((position - entry_pointer_end) as usize);
                println!(
                    "whisky {:?} {} {} {}",
                    Self::to_entry(position, length, references),
                    position,
                    length,
                    references
                );
                free_bytes[..3].copy_from_slice(&Self::to_entry(position, length, references).0);
                // println!("Gar {} {:?}", length, &free_bytes[..3]);

                if has_extra_size {
                    newly_used_bytes[..2].copy_from_slice(&(length as u16).to_le_bytes());
                    newly_used_bytes = &mut newly_used_bytes[2..];
                }
                if has_extra_references {
                    newly_used_bytes[..8].copy_from_slice(&(references as u64).to_le_bytes());
                    newly_used_bytes = &mut newly_used_bytes[8..];
                }
                // println!("roflpi position {}", position);
                Some(BackingPageGuard {
                    tuple: TupleID::with_page_and_slot(
                        self.page_id,
                        (entry_pointer_end as usize - 7) / 3,
                    ),
                    data_length: length + TupleID::SERIALIZED_SIZE * references,
                    data_bytes: newly_used_bytes.as_mut_ptr(),
                    entry_pointer_end_bytes,
                    used_space_start_bytes,
                    new_entry_pointer_end: entry_pointer_end as u16 + 3,
                    new_used_space_start: position as u16,
                    references,
                })
            } else {
                None
            }
        }
    }
}

pub struct NaivePageController<S: SegmentController> {
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
                    Page::from_bytes(page_id, &mut self.segment_controller)?,
                ));
            }
        } else {
            self.current_page = Some((
                false,
                Page::from_bytes(page_id, &mut self.segment_controller)?,
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

impl<'a, S: SegmentController> NaivePageController<S> {
    pub fn get_header(&self) -> &FileHeader {
        &self.header
    }

    pub fn get_entry_bytes(&mut self, tuple: TupleID) -> Result<ByteRange> {
        self.load_page(tuple.page)?;
        let page = self.current_page.as_ref().unwrap();
        page.1
            .get_entry_byte_range(tuple.slot)
            .ok_or_else(|| SlottedPageError::NotFound(tuple))
    }

    pub fn reserve_space(
        &'a mut self,
        references: usize,
        data_length: usize,
    ) -> Result<BackingPageGuard> {
        let length = references * TupleID::SERIALIZED_SIZE + data_length;
        if let Some((dirty, page)) = &mut self.current_page {
            *dirty = true;
            if page.free_space_left() < length {
                self.header.pages += 1;
                self.header_is_dirty = true;
                self.current_page = Some((true, Page::empty(self.header.pages as usize)));
            }
        } else {
            self.header.pages += 1;
            self.header_is_dirty = true;
            self.current_page = Some((true, Page::empty(self.header.pages as usize)));
        }
        Ok(self
            .current_page
            .as_mut()
            .unwrap()
            .1
            .reserve_space(references, data_length)
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
    use std::io::Cursor;

    #[test]
    fn single() {
        let mut bytes = Vec::new();
        {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, 8).unwrap();
            guard
                .reserve_space(8)
                .unwrap()
                .copy_from_slice(&5usize.to_le_bytes());
            guard.commit();
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let bytes = entry_bytes.data_byte_range(..);
            let entry = usize::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);

            assert_eq!(entry, 5);
        }
    }

    #[test]
    fn multiple() {
        let mut bytes = Vec::new();
        {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, 8).unwrap();
            guard
                .reserve_space(8)
                .unwrap()
                .copy_from_slice(&5usize.to_le_bytes());
            guard.commit();
            let mut guard = file.reserve_space(0, 8).unwrap();
            guard
                .reserve_space(8)
                .unwrap()
                .copy_from_slice(&900usize.to_le_bytes());
            guard.commit();
            let mut guard = file.reserve_space(0, 6).unwrap();
            guard
                .reserve_space(6)
                .unwrap()
                .copy_from_slice("roflpi".as_bytes());
            guard.commit();
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let bytes = entry_bytes.data_byte_range(..);
            let entry = usize::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            assert_eq!(entry, 5);
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 1))
                .unwrap();
            let bytes = entry_bytes.data_byte_range(..);
            let entry = usize::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            assert_eq!(entry, 900);
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 2))
                .unwrap();
            let bytes = entry_bytes.data_byte_range(..);
            let entry = std::str::from_utf8(bytes).unwrap();
            assert_eq!(entry, "roflpi");
        }
    }

    #[test]
    fn large() {
        let mut bytes = Vec::new();
        {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, 30000).unwrap();
            guard
                .reserve_space(30000)
                .unwrap()
                .copy_from_slice("lol".repeat(10000).as_bytes());
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let bytes = entry_bytes.data_byte_range(..);
            let entry = std::str::from_utf8(bytes).unwrap();
            println!("wat {}", bytes.len());
            assert_eq!(entry, "lol".repeat(10000));
        }
    }

    #[test]
    fn single_reference() {
        let mut bytes = Vec::new();
        {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(1, 0).unwrap();
            guard
                .add_reference(TupleID::with_page_and_slot(1, 1))
                .unwrap();
            println!("reference saved to: {:?}", guard.commit());
        }
        println!("final bytes: {:?}", &bytes[4096..4096 + 16]);
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            println!("reference bytes: {:?}", &entry_bytes.reference_bytes);
            let entry = entry_bytes.reference(0).unwrap();
            assert_eq!(entry.page, 1);
            assert_eq!(entry.slot, 1);
        }
    }

    #[test]
    fn double_reference() {
        let mut bytes = Vec::new();
        {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(1, 0).unwrap();
            {
                guard
                    .add_reference(TupleID::with_page_and_slot(1, 1))
                    .unwrap();
                println!("reference saved to: {:?}", guard.commit());
            }
            println!(
                "waterday {}",
                file.current_page.as_ref().unwrap().1.used_space_start()
            );
            let mut guard = file.reserve_space(1, 0).unwrap();
            guard
                .add_reference(TupleID::with_page_and_slot(1, 2))
                .unwrap();
            println!("reference saved to: {:?}", guard.commit());
        }
        println!("final bytes: {:?}", &bytes[4096..4096 + 16]);
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            println!("reference bytes: {:?}", &entry_bytes.reference_bytes);
            let entry = entry_bytes.reference(0).unwrap();
            assert_eq!(entry.page, 1);
            assert_eq!(entry.slot, 1);
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 1))
                .unwrap();
            println!("reference bytes: {:?}", &entry_bytes.reference_bytes);
            let entry = entry_bytes.reference(0).unwrap();
            assert_eq!(entry.page, 1);
            assert_eq!(entry.slot, 2);
        }
    }

    #[test]
    fn single_reference_with_large() {
        let mut bytes = Vec::new();
        {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, 30000).unwrap();
            guard
                .reserve_space(30000)
                .unwrap()
                .copy_from_slice("lol".repeat(10000).as_bytes());
            let tuple_id = guard.commit();
            let mut guard = file.reserve_space(1, 0).unwrap();
            guard.add_reference(tuple_id).unwrap();
            println!("reference saved to: {:?}", guard.commit());
        }
        println!("final bytes: {:?}", &bytes[4096..4096 + 16]);
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let entry = std::str::from_utf8(entry_bytes.data_byte_range(..)).unwrap();
            println!("wat {}", entry_bytes.data_byte_range(..).len());
            assert_eq!(entry.len(), 30000);
            assert_eq!(entry, "lol".repeat(10000));
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 1))
                .unwrap();
            let entry = entry_bytes.reference(0).unwrap();
            assert_eq!(entry.page, 1);
            assert_eq!(entry.slot, 0);
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Person {
        name: String,
        occupation: String,
    }

    #[test]
    fn test_complex() {
        let person = Person {
            name: "alice".to_owned(),
            occupation: "blacksmith".to_owned(),
        };

        let mut bytes = Vec::new();
        {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, 8 + 5 + 10).unwrap();
            guard
                .reserve_space(8)
                .unwrap()
                .copy_from_slice(&5usize.to_le_bytes());
            guard
                .reserve_space(person.name.len())
                .unwrap()
                .copy_from_slice(&person.name.as_bytes());
            guard
                .reserve_space(person.occupation.len())
                .unwrap()
                .copy_from_slice(&person.occupation.as_bytes());
            guard.commit();
        }
        println!("final bytes: {:?}", &bytes[4096..4096 + 16]);
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let name_len_bytes = entry_bytes.data_byte_range(0..8);
            println!("bytes {:?}", name_len_bytes);
            let name_len = usize::from_le_bytes([
                name_len_bytes[0],
                name_len_bytes[1],
                name_len_bytes[2],
                name_len_bytes[3],
                name_len_bytes[4],
                name_len_bytes[5],
                name_len_bytes[6],
                name_len_bytes[7],
            ]);
            assert_eq!(name_len, person.name.len());
            let name_bytes = entry_bytes.data_byte_range(8..name_len + 8);
            let name = std::str::from_utf8(name_bytes).unwrap().to_owned();
            assert_eq!(name, person.name);
            let occupation_bytes = entry_bytes.data_byte_range(name_len + 8..);
            let occupation = std::str::from_utf8(occupation_bytes).unwrap().to_owned();
            assert_eq!(occupation, person.occupation);
            let new_person = Person { name, occupation };
            assert_eq!(new_person, person);
        }
    }

    #[test]
    fn test_complex_reference() {
        let person = Person {
            name: "alice".to_owned(),
            occupation: "blacksmith".to_owned(),
        };

        let mut bytes = Vec::new();
        let root_reference = {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, person.name.len()).unwrap();
            guard
                .remaining_data_bytes()
                .copy_from_slice(&person.name.as_bytes());
            let name_reference = guard.commit();
            println!("stored reference for name is {:?}", name_reference);
            let mut guard = file.reserve_space(0, person.occupation.len()).unwrap();
            guard
                .remaining_data_bytes()
                .copy_from_slice(&person.occupation.as_bytes());
            let occupation_reference = guard.commit();
            let mut guard = file.reserve_space(2, 0).unwrap();
            guard.add_reference(name_reference).unwrap();
            guard.add_reference(occupation_reference).unwrap();
            guard.commit()
        };
        println!("final bytes: {:?}", &bytes[4096..4096 + 16]);
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file.get_entry_bytes(root_reference).unwrap();
            let name_reference = entry_bytes.reference(0).unwrap();
            let occupation_reference = entry_bytes.reference(1).unwrap();
            let name_bytes = file.get_entry_bytes(name_reference).unwrap();
            let name = std::str::from_utf8(name_bytes.data_byte_range(..))
                .unwrap()
                .to_owned();
            println!("reference for name is {:?}", name_reference);
            assert_eq!(name, person.name);
            let occupation_bytes = file.get_entry_bytes(occupation_reference).unwrap();
            let occupation = std::str::from_utf8(occupation_bytes.data_byte_range(..))
                .unwrap()
                .to_owned();
            assert_eq!(occupation, person.occupation);
            let new_person = Person { name, occupation };
            assert_eq!(new_person, person);
        }
    }
}
