//! A simple slotted page file format for storing immutable data.

//! # What is the slotted page?
//!
//! The slotted page format is typically used in databases as the underlying storage format for
//! tables and indexes. The file is broken up into a series of pages and each page is made up of
//! slots (and a small header). Entries in the file are stored in the slots. The format is useful
//! for storing linked data such as those found in persistent trees.
//!
//! # Particulars for this library
//!
//! Each file has a root entry that can be queried and overwritten. Anything not reachable from the
//! root entry is considered to be garbage. Garbage entries can still be loaded if requested, but
//! they will be removed upon next compaction.
//!
//! There is no limit to the size of each entry, however, repeatedably inserting many large entries
//! may cause gaps to occur. Additionally, each entry has an additional 3 bytes of overhead.
//!
//! There is a work-in-progress compacter that can help reduce file size without knowledge of the
//! types that are stored in the file.
//!
//! Writes are not currently durable.

#![deny(missing_docs)]

use std::borrow::Cow;
use std::convert::{TryFrom, TryInto};
use std::io::{Read, Seek, SeekFrom, Write};
use std::rc::{Rc, Weak};

use anyhow::anyhow;
use thiserror::Error;

const SEGMENT_SIZE: usize = 4096;

/*
TODO: We should replace this marker with a checksum in the page.
*/
const BACKING_PAGE_MARKER: u8 = 1;

/// A SegmentController is just an extension on Write, Read and Seek types to load segments from
/// files.
pub trait SegmentController {
    /// Read multiple segments from the source. If bytes is not a multiple of a segment size the
    /// remainder of the buffer will not be written to.
    fn read_segments_into(&mut self, start_segment_id: usize, bytes: &mut [u8]) -> Result<()>;

    /// A helper function to load a single segment into a fixed array.
    fn read_segment(&mut self, segment_id: usize) -> Result<[u8; SEGMENT_SIZE]> {
        let mut bytes = [0; SEGMENT_SIZE];
        self.read_segments_into(segment_id, &mut bytes)?;
        Ok(bytes)
    }

    /// Writes multiple segments into the source at the given start segment. Only whole segments
    /// will be written to the sink.
    fn write_segments(&mut self, start_segment_id: usize, bytes: &[u8]) -> Result<()>;
}

/// Implements the helper code for Write + Read + Seek types.
impl<T> SegmentController for T
where
    T: Write + Read + Seek,
{
    fn read_segments_into(&mut self, segment_id: usize, bytes: &mut [u8]) -> Result<()> {
        let offset = segment_id * SEGMENT_SIZE;
        let length = (bytes.len() / SEGMENT_SIZE) * SEGMENT_SIZE;
        let bytes = &mut bytes[..length];
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

/// Represents the errors that are possible for the library.
#[derive(Error, Debug)]
pub enum SlottedPageError {
    /// An error that occurs when loading a file and the header could not be properly loaded.
    #[error("Malformed file header")]
    MalformedFileHeader,

    /// An error when trying to load a page not known by the library or is corrupt.
    #[error("Unknown page type")]
    UnknownPageType(u8),

    /// This is returned when the underlying storage mechanism fails.
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    /// An error for trying to load an nonexistant entry.
    #[error("Entry `{}` not found on page `{}`", .0.slot, .0.page)]
    NotFound(TupleID),

    /// More memory was requested than a guard had available.
    #[error("Insufficient space for request. `{0}` requested, but `{1}` available")]
    InsufficientSpace(usize, usize),

    /// An error occurred during serialization.
    #[error(transparent)]
    SerializationError(anyhow::Error),

    /// An error occurred during deserialization.
    #[error(transparent)]
    DeserializationError(anyhow::Error),
}

type Result<T> = std::result::Result<T, SlottedPageError>;

/// An address within the file that can be used to load an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TupleID {
    page: usize,
    slot: usize,
}

impl TupleID {
    const SERIALIZED_SIZE: usize = 10;

    fn with_page_and_slot(page: usize, slot: usize) -> Self {
        TupleID { page, slot }
    }

    fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut result = [0; Self::SERIALIZED_SIZE];
        result[..8].copy_from_slice(&(self.page as u64).to_le_bytes());
        result[8..].copy_from_slice(&(self.slot as u16).to_le_bytes());
        result
    }

    fn from_le_bytes(bytes: [u8; Self::SERIALIZED_SIZE]) -> Self {
        TupleID::with_page_and_slot(
            u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize,
            u16::from_le_bytes(bytes[8..].try_into().unwrap()) as usize,
        )
    }
}

/// A file header to keep various statistics on the file and its entries.
pub struct FileHeader {
    pages: usize,
    /// The root tuple id of the file.
    pub root: Option<TupleID>,
}

impl FileHeader {
    fn new() -> Self {
        FileHeader {
            pages: 0,
            root: None,
        }
    }

    fn from_bytes(bytes: [u8; SEGMENT_SIZE]) -> Result<Self> {
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

    fn to_bytes(&self) -> [u8; SEGMENT_SIZE] {
        let mut bytes = [0u8; SEGMENT_SIZE];
        bytes[0..5].copy_from_slice(b"MAGIC");
        bytes[5..13].copy_from_slice(&(self.pages as u64).to_le_bytes());
        bytes
    }
}

/// A guard for writing data that safely updates the relevant items once committed.
#[derive(Debug)]
pub struct Guard<'a> {
    tuple: TupleID,
    data_bytes: *mut u8,
    data_length: usize,
    references: usize,
    new_entry_pointer_end: u16,
    entry_pointer_end_bytes: &'a mut [u8],
    new_used_space_start: u16,
    used_space_start_bytes: &'a mut [u8],
}

impl<'a> Guard<'a> {
    /// Adds a reference to the guard.
    pub fn add_reference(&mut self, tuple_id: TupleID) -> Result<()> {
        let serialized_reference = tuple_id.to_le_bytes();
        let data_bytes =
            unsafe { std::slice::from_raw_parts_mut(self.data_bytes, self.data_length) };
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

    /// Reserves an amount of data bytes from the guard.
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

    /// Reserves all the remaining data bytes from the guard.
    pub fn remaining_data_bytes(&mut self) -> &mut [u8] {
        let available = self.data_length - self.references * TupleID::SERIALIZED_SIZE;
        let data_bytes = unsafe { std::slice::from_raw_parts_mut(self.data_bytes, available) };
        self.data_length -= available;
        &mut data_bytes[self.data_length..]
    }

    fn inner_commit(&mut self) {
        self.entry_pointer_end_bytes
            .copy_from_slice(&self.new_entry_pointer_end.to_le_bytes());
        self.used_space_start_bytes
            .copy_from_slice(&self.new_used_space_start.to_le_bytes());
    }

    /// Commits the changes (not necessarily to storage) and returns a reference that can be used
    /// to reload the entry.
    pub fn commit(self) -> TupleID {
        self.tuple
    }
}

impl<'a> Drop for Guard<'a> {
    fn drop(&mut self) {
        self.inner_commit();
    }
}

/// An iterator over the references that are available in a ByteRange.
pub struct ByteRangeReferenceIter<'a> {
    bytes: &'a [u8],
    front: usize,
    back: usize,
}

impl<'a> Iterator for ByteRangeReferenceIter<'a> {
    type Item = TupleID;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front != self.back {
            self.front += TupleID::SERIALIZED_SIZE;
            let start = self.bytes.len() - self.front;
            Some(TupleID::from_le_bytes([
                self.bytes[start + 0],
                self.bytes[start + 1],
                self.bytes[start + 2],
                self.bytes[start + 3],
                self.bytes[start + 4],
                self.bytes[start + 5],
                self.bytes[start + 6],
                self.bytes[start + 7],
                self.bytes[start + 8],
                self.bytes[start + 9],
            ]))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = (self.back - self.front) / TupleID::SERIALIZED_SIZE;
        (size, Some(size))
    }
}

impl<'a> DoubleEndedIterator for ByteRangeReferenceIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front != self.back {
            self.back -= TupleID::SERIALIZED_SIZE;
            let start = self.bytes.len() - self.back;
            Some(TupleID::from_le_bytes([
                self.bytes[start + 0],
                self.bytes[start + 1],
                self.bytes[start + 2],
                self.bytes[start + 3],
                self.bytes[start + 4],
                self.bytes[start + 5],
                self.bytes[start + 6],
                self.bytes[start + 7],
                self.bytes[start + 8],
                self.bytes[start + 9],
            ]))
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for ByteRangeReferenceIter<'a> {}

/// Represents the data and references that are stored for a entry.
pub struct ByteRange<'a> {
    data_bytes: &'a [u8],
    reference_bytes: &'a [u8],
}

impl<'a> ByteRange<'a> {
    fn from_bytes(bytes: &'a [u8], references: usize) -> Self {
        let reference_start = bytes.len() - TupleID::SERIALIZED_SIZE * references;
        let (data_bytes, reference_bytes) = bytes.split_at(reference_start);
        ByteRange {
            data_bytes,
            reference_bytes,
        }
    }

    /// Gets the reference at the given index in the byte range.
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

    /// Returns an iterator over the references in the byte range.
    pub fn references(&self) -> ByteRangeReferenceIter<'a> {
        ByteRangeReferenceIter {
            bytes: self.reference_bytes,
            front: 0,
            back: self.reference_bytes.len(),
        }
    }

    /// Returns the data bytes in the byte range.
    pub fn data_bytes(&self) -> &[u8] {
        &self.data_bytes
    }

    /// Splits the byte range into two byte ranges at the given position.
    pub fn split_at(self, reference_position: usize, data_position: usize) -> (Self, Self) {
        let (new_data_bytes, split_data_bytes) = self.data_bytes.split_at(data_position);
        let (split_reference_bytes, new_reference_bytes) = self
            .reference_bytes
            .split_at(reference_position * TupleID::SERIALIZED_SIZE);

        (
            Self {
                reference_bytes: new_reference_bytes,
                data_bytes: new_data_bytes,
            },
            Self {
                reference_bytes: split_reference_bytes,
                data_bytes: split_data_bytes,
            },
        )
    }
}

// An extended page holds only a single item. The length of this item is stored in the first four
// bytes. The number of pages that compose the the extended page is calculated as:
// (length + 10) / SEGMENT_SIZE rounded up.
// Each entry thing is 24 bits:
// 012345678901234567890123
// ppppppppppppssssssssxxxx
// Position is 12 bytes for exact positioning
// Size is 8 bits: 255 means the size that is kept in the first 2 bytes of the payload
// Reference counter is 4 bits: 15 means the number is kept in the first 8 bytes of the payload
struct Page {
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
        bytes.resize_with(SEGMENT_SIZE * (additional_segments + 1), Default::default);
        controller.read_segments_into(page_id + 1, &mut bytes[SEGMENT_SIZE..])?;
        Ok(Page { page_id, bytes })
    }

    fn total_required_size(data: usize, references: usize) -> (usize, bool, bool) {
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

    fn to_entry(position: usize, size: usize, references: usize) -> ([u8; 3], bool, bool) {
        assert!(position < SEGMENT_SIZE);
        assert!(size < SEGMENT_SIZE);

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

    fn entry_pointer_end(&self) -> usize {
        u16::from_ne_bytes([self.bytes[3], self.bytes[4]]) as usize
    }

    fn num_entries(&self) -> usize {
        (self.entry_pointer_end() - 7) / 3
    }

    fn unpack_entry(&self, entry: usize) -> Option<(usize, usize, usize)> {
        if entry < self.num_entries() {
            let entry_start = 7 + 3 * entry;
            let packed_entry = [
                self.bytes[entry_start],
                self.bytes[entry_start + 1],
                self.bytes[entry_start + 2],
            ];
            let (mut offset, mut size, mut references) = Self::from_entry(packed_entry);
            if size == 255 {
                size = u16::from_ne_bytes([self.bytes[offset], self.bytes[offset + 1]]) as usize;
                offset += 2;
            }
            if entry == 0 {
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
        self.unpack_entry(entry).map(|(offset, size, references)| {
            let bytes = &self.bytes[offset..offset + size + TupleID::SERIALIZED_SIZE * references];
            ByteRange::from_bytes(bytes, references)
        })
    }

    fn used_space_start(&self) -> usize {
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

    fn reserve_space(&mut self, references: usize, length: usize) -> Option<Guard> {
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
            remaining_bytes[..3]
                .copy_from_slice(&Self::to_entry(position, written_size, references).0);

            let bytes_start = remaining_bytes.len() - total_data_size;
            let mut bytes = &mut remaining_bytes[bytes_start..];
            if has_extra_size {
                bytes[..2].copy_from_slice(&(written_size as u16).to_le_bytes());
                bytes = &mut bytes[2..];
            }
            if has_extra_references {
                bytes[..8].copy_from_slice(&(references as u64).to_le_bytes());
                bytes = &mut bytes[8..];
            }
            Some(Guard {
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
            let total_required_space = data_required_space + 3;

            if available_space >= total_required_space {
                let position = used_space_start - data_required_space;
                let (free_bytes, mut newly_used_bytes) =
                    remaining_bytes.split_at_mut((position - entry_pointer_end) as usize);
                free_bytes[..3].copy_from_slice(&Self::to_entry(position, length, references).0);

                if has_extra_size {
                    newly_used_bytes[..2].copy_from_slice(&(length as u16).to_le_bytes());
                    newly_used_bytes = &mut newly_used_bytes[2..];
                }
                if has_extra_references {
                    newly_used_bytes[..8].copy_from_slice(&(references as u64).to_le_bytes());
                    newly_used_bytes = &mut newly_used_bytes[8..];
                }
                Some(Guard {
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

/// A controller object that represents the file. The controller object keeps track of the currently
/// loaded page and keeping the header up to date.
pub struct NaivePageController<S: SegmentController> {
    header: FileHeader,
    header_is_dirty: bool,
    segment_controller: S,
    current_page: Option<(bool, Page)>,
}

impl<S: SegmentController> NaivePageController<S> {
    /// Creates a new controller for a new file.
    pub fn from_new(segment_controller: S) -> Result<Self> {
        let header = FileHeader::new();
        Ok(NaivePageController {
            header,
            header_is_dirty: true,
            segment_controller,
            current_page: None,
        })
    }

    /// Creates a new controller for an existing file.
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
    /// Gets the header for the file
    pub fn get_header(&self) -> &FileHeader {
        &self.header
    }

    /// Returns the data associated with the given entry.
    pub fn get_entry_bytes(&mut self, tuple: TupleID) -> Result<ByteRange> {
        self.load_page(tuple.page)?;
        let page = self.current_page.as_ref().unwrap();
        page.1
            .get_entry_byte_range(tuple.slot)
            .ok_or_else(|| SlottedPageError::NotFound(tuple))
    }

    /// Reserves space for an item to be written. The length must be given in the number of
    /// references and the number of data bytes required.
    pub fn reserve_space(&'a mut self, references: usize, data_length: usize) -> Result<Guard> {
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

/// A trait for types that can be loaded from a file and deserialized without any other information.
pub trait Loadable {
    /// The resultant type that is deserialized from the file.
    type LoadType;

    /// Loads the item from the file.
    fn load<S: SegmentController>(
        self,
        file: &mut NaivePageController<S>,
    ) -> Result<Self::LoadType>;
}

/// A trait for types that can be written to the file.
pub trait Serialize {
    /// The data length required for the object.
    fn data_length(&self) -> usize;

    /// Serializes all the relevant references. The TupleIDs should be stored in the references
    /// vector. The references will be stored in the order given in the vector.
    fn serialize_references<'a, S: SegmentController>(
        &self,
        file: &'a mut NaivePageController<S>,
        references: &mut Vec<TupleID>,
    ) -> Result<()>;

    /// Serializes the data part of the object. The guard passed in will have at least data_length()
    /// data bytes free.  
    fn serialize_data(&self, guard: &mut Guard) -> Result<()>;

    /// Serializes the object to file.
    fn serialize<S: SegmentController>(
        &self,
        file: &mut NaivePageController<S>,
    ) -> Result<TupleID> {
        // Behaviour of serialize
        // 1) Calculate the total structure size by summing each field's size.
        // 2) Loop through each field and serialize references to TupleIDs.
        // 3) Get a guard for the total size.
        // 4) Add all the references to the guard.
        // 5) Add all the data.
        // 6) Commit the guard to get a resultant TupleID.
        // The easy way involves an allocation for all of the tuple id's, but we should investigate
        // a way to avoid this. This would basically entail having backing page not borrow file
        // mutably which might be possible through atomics. OTOH we might not want to do this to
        // ensure that we can bound memory resources.
        let mut references = Vec::new();
        let data_length = self.data_length();
        self.serialize_references(file, &mut references)?;
        let mut guard = file.reserve_space(references.len(), data_length)?;
        for tuple_id in references.drain(..) {
            guard.add_reference(tuple_id)?;
        }
        self.serialize_data(&mut guard)?;
        Ok(guard.commit())
    }
}

/// A wrapper type to indicate that loading is not necessary for a type. Objects wrapped in this
/// type will be return verbatim when loading.
pub struct LoadNotNecessary<T: for<'a> TryFrom<ByteRange<'a>, Error = anyhow::Error>>(T);

impl<T: for<'a> TryFrom<ByteRange<'a>, Error = anyhow::Error>> Loadable for LoadNotNecessary<T> {
    type LoadType = T;

    fn load<S: SegmentController>(
        self,
        _file: &mut NaivePageController<S>,
    ) -> Result<Self::LoadType> {
        Ok(self.0)
    }
}

impl<'b, T: for<'a> TryFrom<ByteRange<'a>, Error = anyhow::Error>> TryFrom<ByteRange<'b>>
    for LoadNotNecessary<T>
{
    type Error = anyhow::Error;

    fn try_from(t: ByteRange<'b>) -> std::result::Result<Self, anyhow::Error> {
        Ok(LoadNotNecessary(T::try_from(t)?))
    }
}

/// A TupleID that is equiped with a type for convenient deserialization into the type.
pub struct TypedTupleID<T: Deserialize> {
    tuple: TupleID,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Deserialize> Loadable for TypedTupleID<T> {
    type LoadType = T;

    fn load<S: SegmentController>(
        self,
        file: &mut NaivePageController<S>,
    ) -> Result<Self::LoadType> {
        T::deserialize(file, self.tuple)
    }
}

/// A trait for items that can be deserialized. The difference between Deserialize and Loadable is
/// that Deserialize takes in an additional entry ID to deserialize, while Loadable does not.
pub trait Deserialize: Sized {
    /// The LoadType is the proto representation of the Self, but one that can be loaded into Self.
    /// The LoadType cannot borrow items from the ByteRange.
    type LoadType: Loadable<LoadType = Self> + for<'a> TryFrom<ByteRange<'a>, Error = anyhow::Error>;

    /// Deserializes the given entry ID into an object.
    fn deserialize<S: SegmentController>(
        file: &mut NaivePageController<S>,
        tuple: TupleID,
    ) -> Result<Self> {
        let x: Self::LoadType = file
            .get_entry_bytes(tuple)?
            .try_into()
            .map_err(|x: anyhow::Error| SlottedPageError::DeserializationError(x.into()))?;
        x.load(file)
    }
}

impl Serialize for bool {
    fn data_length(&self) -> usize {
        1
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(1)?[0] = *self as u8;
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for bool {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        if value.data_bytes()[0] == 0 {
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

impl Deserialize for bool {
    type LoadType = LoadNotNecessary<bool>;
}

impl<'a> TryFrom<ByteRange<'a>> for u8 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(value.data_bytes()[0])
    }
}

impl Serialize for u8 {
    fn data_length(&self) -> usize {
        1
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(1)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl Deserialize for u8 {
    type LoadType = LoadNotNecessary<u8>;
}

impl Serialize for u16 {
    fn data_length(&self) -> usize {
        2
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(2)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for u16 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for u16 {
    type LoadType = LoadNotNecessary<u16>;
}

impl Serialize for u32 {
    fn data_length(&self) -> usize {
        4
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(4)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for u32 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for u32 {
    type LoadType = LoadNotNecessary<u32>;
}

impl Serialize for u64 {
    fn data_length(&self) -> usize {
        8
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(8)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for u64 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for u64 {
    type LoadType = LoadNotNecessary<u64>;
}

impl Serialize for usize {
    fn data_length(&self) -> usize {
        8
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(8)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for usize {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for usize {
    type LoadType = LoadNotNecessary<usize>;
}

impl Serialize for i8 {
    fn data_length(&self) -> usize {
        1
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(1)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for i8 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for i8 {
    type LoadType = LoadNotNecessary<i8>;
}

impl Serialize for i16 {
    fn data_length(&self) -> usize {
        2
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(2)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for i16 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for i16 {
    type LoadType = LoadNotNecessary<i16>;
}

impl Serialize for i32 {
    fn data_length(&self) -> usize {
        4
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(4)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for i32 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for i32 {
    type LoadType = LoadNotNecessary<i32>;
}

impl Serialize for i64 {
    fn data_length(&self) -> usize {
        8
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(8)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for i64 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for i64 {
    type LoadType = LoadNotNecessary<i64>;
}

impl Serialize for isize {
    fn data_length(&self) -> usize {
        8
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(8)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for isize {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for isize {
    type LoadType = LoadNotNecessary<isize>;
}

impl Serialize for f32 {
    fn data_length(&self) -> usize {
        4
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(4)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for f32 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for f32 {
    type LoadType = LoadNotNecessary<f32>;
}

impl Serialize for f64 {
    fn data_length(&self) -> usize {
        8
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard.reserve_space(8)?.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for f64 {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        Ok(Self::from_le_bytes(value.data_bytes().try_into().map_err(
            |_| SlottedPageError::DeserializationError(anyhow!("Unexpected bytes size")),
        )?))
    }
}

impl Deserialize for f64 {
    type LoadType = LoadNotNecessary<f64>;
}

impl Serialize for char {
    fn data_length(&self) -> usize {
        self.len_utf8()
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        self.encode_utf8(guard.reserve_space(self.data_length())?);
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for char {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        let bytes = value.data_bytes();
        Ok(std::str::from_utf8(bytes)
            .map_err(|x| SlottedPageError::SerializationError(x.into()))?
            .chars()
            .next()
            .unwrap())
    }
}

impl Deserialize for char {
    type LoadType = LoadNotNecessary<char>;
}

impl Serialize for str {
    fn data_length(&self) -> usize {
        self.as_bytes().len()
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard
            .reserve_space(self.data_length())?
            .copy_from_slice(&self.as_bytes());
        Ok(())
    }
}

impl Serialize for String {
    fn data_length(&self) -> usize {
        self.as_bytes().len()
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        _references: &mut Vec<TupleID>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
        guard
            .reserve_space(self.data_length())?
            .copy_from_slice(&self.as_bytes());
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for String {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        let bytes = value.data_bytes();
        Ok(std::str::from_utf8(bytes)
            .map_err(|x| SlottedPageError::SerializationError(x.into()))?
            .to_owned())
    }
}

impl Deserialize for String {
    type LoadType = LoadNotNecessary<String>;
}

impl<T: Serialize> Serialize for Box<T> {
    fn data_length(&self) -> usize {
        0
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        file: &'a mut NaivePageController<S>,
        references: &mut Vec<TupleID>,
    ) -> Result<()> {
        references.push(self.as_ref().serialize(file)?);
        Ok(())
    }

    fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
        Ok(())
    }
}

impl<'a, T: Deserialize> TryFrom<ByteRange<'a>> for TypedTupleID<T> {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        let reference = value.reference(0).unwrap();
        Ok(TypedTupleID {
            tuple: reference,
            _marker: std::marker::PhantomData,
        })
    }
}

/// A wrapper type to load an item into a Box.
pub struct BoxedTupleID<T: Deserialize>(TypedTupleID<T>);

impl<'a, T: Deserialize> TryFrom<ByteRange<'a>> for BoxedTupleID<T> {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        let reference = value.reference(0).unwrap();
        Ok(BoxedTupleID(TypedTupleID {
            tuple: reference,
            _marker: std::marker::PhantomData,
        }))
    }
}

impl<T: Deserialize> Loadable for BoxedTupleID<T> {
    type LoadType = Box<T>;

    fn load<S: SegmentController>(
        self,
        file: &mut NaivePageController<S>,
    ) -> Result<Self::LoadType> {
        Ok(Box::new(self.0.load(file)?))
    }
}

impl<T: Deserialize> Deserialize for Box<T> {
    type LoadType = BoxedTupleID<T>;
}

/// A wrapper type to load an item into a Rc.
pub struct RcedTupleID<T: Deserialize>(TypedTupleID<T>);

impl<'a, T: Deserialize> TryFrom<ByteRange<'a>> for RcedTupleID<T> {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        let reference = value.reference(0).unwrap();
        Ok(RcedTupleID(TypedTupleID {
            tuple: reference,
            _marker: std::marker::PhantomData,
        }))
    }
}

impl<T: Deserialize> Loadable for RcedTupleID<T> {
    type LoadType = Rc<T>;

    fn load<S: SegmentController>(
        self,
        file: &mut NaivePageController<S>,
    ) -> Result<Self::LoadType> {
        Ok(Rc::new(self.0.load(file)?))
    }
}

impl<T: Serialize> Serialize for Rc<T> {
    fn data_length(&self) -> usize {
        0
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        file: &'a mut NaivePageController<S>,
        references: &mut Vec<TupleID>,
    ) -> Result<()> {
        references.push(self.as_ref().serialize(file)?);
        Ok(())
    }

    fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
        Ok(())
    }
}

impl<T: Deserialize> Deserialize for Rc<T> {
    type LoadType = RcedTupleID<T>;
}

impl<T: Serialize> Serialize for Weak<T> {
    fn data_length(&self) -> usize {
        0
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        file: &'a mut NaivePageController<S>,
        references: &mut Vec<TupleID>,
    ) -> Result<()> {
        references.push(self.upgrade().unwrap().as_ref().serialize(file)?);
        Ok(())
    }

    fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
        Ok(())
    }
}

impl Serialize for TupleID {
    fn data_length(&self) -> usize {
        0
    }

    fn serialize_references<'a, S: SegmentController>(
        &self,
        _file: &'a mut NaivePageController<S>,
        references: &mut Vec<TupleID>,
    ) -> Result<()> {
        references.push(*self);
        Ok(())
    }

    fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
        Ok(())
    }
}

impl<'a> TryFrom<ByteRange<'a>> for TupleID {
    type Error = anyhow::Error;

    fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
        let reference = value.reference(0).unwrap();
        Ok(reference)
    }
}

impl Deserialize for TupleID {
    type LoadType = LoadNotNecessary<TupleID>;
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
            let bytes = entry_bytes.data_bytes();
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
            let bytes = entry_bytes.data_bytes();
            let entry = usize::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            assert_eq!(entry, 5);
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 1))
                .unwrap();
            let bytes = entry_bytes.data_bytes();
            let entry = usize::from_ne_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]);
            assert_eq!(entry, 900);
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 2))
                .unwrap();
            let bytes = entry_bytes.data_bytes();
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
            let bytes = entry_bytes.data_bytes();
            let entry = std::str::from_utf8(bytes).unwrap();
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
            guard.commit();
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
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
                guard.commit();
            }
            let mut guard = file.reserve_space(1, 0).unwrap();
            guard
                .add_reference(TupleID::with_page_and_slot(1, 2))
                .unwrap();
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let entry = entry_bytes.reference(0).unwrap();
            assert_eq!(entry.page, 1);
            assert_eq!(entry.slot, 1);
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 1))
                .unwrap();
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
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let entry = std::str::from_utf8(entry_bytes.data_bytes()).unwrap();
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
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(TupleID::with_page_and_slot(1, 0))
                .unwrap();
            let name_len_bytes = &entry_bytes.data_bytes()[..8];
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
            let name_bytes = &entry_bytes.data_bytes()[8..name_len + 8];
            let name = std::str::from_utf8(name_bytes).unwrap().to_owned();
            assert_eq!(name, person.name);
            let occupation_bytes = &entry_bytes.data_bytes[name_len + 8..];
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
        let (root_reference, name_reference, occupation_reference) = {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, person.name.len()).unwrap();
            guard
                .remaining_data_bytes()
                .copy_from_slice(&person.name.as_bytes());
            let name_reference = guard.commit();
            let mut guard = file.reserve_space(0, person.occupation.len()).unwrap();
            guard
                .remaining_data_bytes()
                .copy_from_slice(&person.occupation.as_bytes());
            let occupation_reference = guard.commit();
            let mut guard = file.reserve_space(2, 0).unwrap();
            guard.add_reference(name_reference).unwrap();
            guard.add_reference(occupation_reference).unwrap();
            (guard.commit(), name_reference, occupation_reference)
        };
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file.get_entry_bytes(root_reference).unwrap();
            assert_eq!(
                entry_bytes.references().collect::<Vec<_>>(),
                vec![name_reference, occupation_reference]
            );
            let input_name_reference = entry_bytes.reference(0).unwrap();
            assert_eq!(input_name_reference, name_reference);
            let input_occupation_reference = entry_bytes.reference(1).unwrap();
            assert_eq!(input_occupation_reference, occupation_reference);
            let name_bytes = file.get_entry_bytes(name_reference).unwrap();
            let name = std::str::from_utf8(name_bytes.data_bytes())
                .unwrap()
                .to_owned();
            assert_eq!(name, person.name);
            let occupation_bytes = file.get_entry_bytes(occupation_reference).unwrap();
            let occupation = std::str::from_utf8(occupation_bytes.data_bytes())
                .unwrap()
                .to_owned();
            assert_eq!(occupation, person.occupation);
            let new_person = Person { name, occupation };
            assert_eq!(new_person, person);
        }
    }

    #[test]
    fn single_serialize() {
        let mut bytes = Vec::new();
        let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
        let tuple_id = 5usize.serialize(&mut file).unwrap();
        let entry = usize::deserialize(&mut file, tuple_id).unwrap();
        assert_eq!(entry, 5);
    }

    #[test]
    fn multiple_serialize() {
        let mut bytes = Vec::new();

        let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
        let tuple_5 = 5usize.serialize(&mut file).unwrap();
        let tuple_900 = 900usize.serialize(&mut file).unwrap();
        let tuple_roflpi = "roflpi".serialize(&mut file).unwrap();

        let entry_5 = usize::deserialize(&mut file, tuple_5).unwrap();
        assert_eq!(entry_5, 5);
        let entry_900 = usize::deserialize(&mut file, tuple_900).unwrap();
        assert_eq!(entry_900, 900);
        let entry_roflpi = String::deserialize(&mut file, tuple_roflpi).unwrap();
        assert_eq!(entry_roflpi, "roflpi");
    }

    #[test]
    fn large_serialize() {
        let mut bytes = Vec::new();
        let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
        let tuple_id = "lol".repeat(10000).serialize(&mut file).unwrap();
        let entry = String::deserialize(&mut file, tuple_id).unwrap();
        assert_eq!(entry, "lol".repeat(10000));
    }

    #[test]
    fn single_reference_serialize() {
        let mut bytes = Vec::new();
        let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
        let tuple_id = Box::new(5usize).serialize(&mut file).unwrap();
        let entry: Box<usize> = Box::deserialize(&mut file, tuple_id).unwrap();
        assert_eq!(entry.as_ref(), &5);
    }

    #[test]
    fn double_reference_serialize() {
        let mut bytes = Vec::new();
        let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
        let num_id = 5.serialize(&mut file).unwrap();
        let ref1 = num_id.serialize(&mut file).unwrap();
        let ref2 = num_id.serialize(&mut file).unwrap();
        assert_ne!(ref1, ref2);
        let ref_1_val = TupleID::deserialize(&mut file, ref1).unwrap();
        let ref_2_val = TupleID::deserialize(&mut file, ref2).unwrap();
        assert_eq!(ref_1_val, ref_2_val);
        let entry = i32::deserialize(&mut file, ref_1_val).unwrap();
        assert_eq!(entry, 5);
    }

    #[test]
    fn single_reference_with_large_serialize() {
        let mut bytes = Vec::new();
        let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
        let large_id = "lol".repeat(10000).serialize(&mut file).unwrap();
        let reference_id = large_id.serialize(&mut file).unwrap();
        assert_ne!(large_id, reference_id);

        let reference_value = TupleID::deserialize(&mut file, reference_id).unwrap();
        assert_eq!(reference_value, large_id);

        let large_value = String::deserialize(&mut file, reference_value).unwrap();
        assert_eq!(large_value, "lol".repeat(10000));
    }

    #[test]
    fn test_complex_serialize() {
        impl Serialize for Person {
            fn data_length(&self) -> usize {
                8 + self.name.len() + self.occupation.len()
            }

            fn serialize_references<'a, S: SegmentController>(
                &self,
                _file: &'a mut NaivePageController<S>,
                _references: &mut Vec<TupleID>,
            ) -> Result<()> {
                Ok(())
            }

            fn serialize_data(&self, guard: &mut Guard) -> Result<()> {
                self.name.len().serialize_data(guard)?;
                self.name.serialize_data(guard)?;
                self.occupation.serialize_data(guard)
            }
        }

        impl<'a> TryFrom<ByteRange<'a>> for Person {
            type Error = anyhow::Error;

            fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
                // Need way to deserialize sub entries from the relevant things.
                let (name_len_range, remaining) = value.split_at(0, 8);
                let name_len = name_len_range.try_into()?;
                let (name_range, occupation_range) = remaining.split_at(0, name_len);
                let name = name_range.try_into()?;
                let occupation = occupation_range.try_into()?;
                Ok(Person { name, occupation })
            }
        }

        impl Deserialize for Person {
            type LoadType = LoadNotNecessary<Person>;
        }
        let person = Person {
            name: "alice".to_owned(),
            occupation: "blacksmith".to_owned(),
        };

        let mut bytes = Vec::new();
        let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
        let person_id = person.serialize(&mut file).unwrap();
        let entry = Person::deserialize(&mut file, person_id).unwrap();
        assert_eq!(entry, person);
    }

    #[test]
    fn test_complex_reference_serialize() {
        #[derive(Debug, PartialEq, Eq)]
        struct Person {
            name: String,
            occupation: String,
        }

        struct LazyPerson {
            name: TypedTupleID<String>,
            occupation: TypedTupleID<String>,
        }

        impl<'a> TryFrom<ByteRange<'a>> for LazyPerson {
            type Error = anyhow::Error;

            fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
                let name = value.reference(0).unwrap();
                let occupation = value.reference(1).unwrap();
                Ok(LazyPerson {
                    name: TypedTupleID {
                        tuple: name,
                        _marker: std::marker::PhantomData,
                    },
                    occupation: TypedTupleID {
                        tuple: occupation,
                        _marker: std::marker::PhantomData,
                    },
                })
            }
        }

        impl Loadable for LazyPerson {
            type LoadType = Person;

            fn load<S: SegmentController>(
                self,
                file: &mut NaivePageController<S>,
            ) -> Result<Self::LoadType> {
                Ok(Person {
                    name: self.name.load(file)?,
                    occupation: self.occupation.load(file)?,
                })
            }
        }

        impl Serialize for Person {
            fn data_length(&self) -> usize {
                0
            }

            fn serialize_references<'a, S: SegmentController>(
                &self,
                file: &'a mut NaivePageController<S>,
                references: &mut Vec<TupleID>,
            ) -> Result<()> {
                references.push(self.name.serialize(file)?);
                references.push(self.occupation.serialize(file)?);
                Ok(())
            }

            fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
                Ok(())
            }
        }

        impl Deserialize for Person {
            type LoadType = LazyPerson;
        }

        let person = Person {
            name: "alice".to_owned(),
            occupation: "blacksmith".to_owned(),
        };

        let mut bytes = Vec::new();
        let (root_reference, name_reference, occupation_reference) = {
            let mut file = NaivePageController::from_new(Cursor::new(&mut bytes)).unwrap();
            let mut guard = file.reserve_space(0, person.name.len()).unwrap();
            guard
                .remaining_data_bytes()
                .copy_from_slice(&person.name.as_bytes());
            let name_reference = guard.commit();
            let mut guard = file.reserve_space(0, person.occupation.len()).unwrap();
            guard
                .remaining_data_bytes()
                .copy_from_slice(&person.occupation.as_bytes());
            let occupation_reference = guard.commit();
            let mut guard = file.reserve_space(2, 0).unwrap();
            guard.add_reference(name_reference).unwrap();
            guard.add_reference(occupation_reference).unwrap();
            (guard.commit(), name_reference, occupation_reference)
        };
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file.get_entry_bytes(root_reference).unwrap();
            assert_eq!(
                entry_bytes.references().collect::<Vec<_>>(),
                vec![name_reference, occupation_reference]
            );
            let input_name_reference = entry_bytes.reference(0).unwrap();
            assert_eq!(input_name_reference, name_reference);
            let input_occupation_reference = entry_bytes.reference(1).unwrap();
            assert_eq!(input_occupation_reference, occupation_reference);
            let name_bytes = file.get_entry_bytes(name_reference).unwrap();
            let name = std::str::from_utf8(name_bytes.data_bytes())
                .unwrap()
                .to_owned();
            assert_eq!(name, person.name);
            let occupation_bytes = file.get_entry_bytes(occupation_reference).unwrap();
            let occupation = std::str::from_utf8(occupation_bytes.data_bytes())
                .unwrap()
                .to_owned();
            assert_eq!(occupation, person.occupation);
            let new_person = Person { name, occupation };
            assert_eq!(new_person, person);
        }
    }
}
