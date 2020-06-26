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

pub mod controller;
pub mod error;
pub mod serde;

use std::borrow::Cow;
use std::convert::TryInto;

use crate::controller::SegmentController;
// pub use crate::controller;
use crate::error::{Result, SlottedPageError};
// pub use crate::serde::*;

const SEGMENT_SIZE: usize = 4096;

/*
TODO: We should replace this marker with a checksum in the page.
*/
const BACKING_PAGE_MARKER: u8 = 1;

/// An address within the file that can be used to load an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntryID {
    page: usize,
    slot: usize,
}

impl EntryID {
    const SERIALIZED_SIZE: usize = 10;

    fn with_page_and_slot(page: usize, slot: usize) -> Self {
        EntryID { page, slot }
    }

    fn to_le_bytes(&self) -> [u8; Self::SERIALIZED_SIZE] {
        let mut result = [0; Self::SERIALIZED_SIZE];
        result[..8].copy_from_slice(&(self.page as u64).to_le_bytes());
        result[8..].copy_from_slice(&(self.slot as u16).to_le_bytes());
        result
    }

    fn from_le_bytes(bytes: [u8; Self::SERIALIZED_SIZE]) -> Self {
        EntryID::with_page_and_slot(
            u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize,
            u16::from_le_bytes(bytes[8..].try_into().unwrap()) as usize,
        )
    }
}

/// A file header to keep various statistics on the file and its entries.
pub struct FileHeader {
    pages: usize,
    /// The root entry id of the file.
    pub root: Option<EntryID>,
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
            let pages = u64::from_le_bytes(bytes[5..13].try_into().unwrap()) as usize;
            let root = if bytes[13] != 0 {
                Some(EntryID::from_le_bytes(
                    bytes[14..14 + EntryID::SERIALIZED_SIZE].try_into().unwrap(),
                ))
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
    tuple: EntryID,
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
    pub fn add_reference(&mut self, tuple_id: EntryID) -> Result<()> {
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
        let available = self.data_length - self.references * EntryID::SERIALIZED_SIZE;
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
        let available = self.data_length - self.references * EntryID::SERIALIZED_SIZE;
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
    pub fn commit(self) -> EntryID {
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
    type Item = EntryID;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front != self.back {
            self.front += EntryID::SERIALIZED_SIZE;
            let start = self.bytes.len() - self.front;
            Some(EntryID::from_le_bytes(
                self.bytes[start..start + EntryID::SERIALIZED_SIZE]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = (self.back - self.front) / EntryID::SERIALIZED_SIZE;
        (size, Some(size))
    }
}

impl<'a> DoubleEndedIterator for ByteRangeReferenceIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front != self.back {
            self.back -= EntryID::SERIALIZED_SIZE;
            let start = self.bytes.len() - self.back;
            Some(EntryID::from_le_bytes(
                self.bytes[start..start + EntryID::SERIALIZED_SIZE]
                    .try_into()
                    .unwrap(),
            ))
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
        let reference_start = bytes.len() - EntryID::SERIALIZED_SIZE * references;
        let (data_bytes, reference_bytes) = bytes.split_at(reference_start);
        ByteRange {
            data_bytes,
            reference_bytes,
        }
    }

    /// Gets the reference at the given index in the byte range.
    pub fn reference(&self, index: usize) -> Option<EntryID> {
        let reference_end = self.reference_bytes.len() - index * EntryID::SERIALIZED_SIZE;
        let bytes = &self.reference_bytes[reference_end - EntryID::SERIALIZED_SIZE..reference_end];
        if bytes.len() == EntryID::SERIALIZED_SIZE {
            Some(EntryID::from_le_bytes(bytes.try_into().unwrap()))
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
            .split_at(reference_position * EntryID::SERIALIZED_SIZE);

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
        let additional_segments = u16::from_le_bytes(bytes[1..3].try_into().unwrap()) as usize;
        bytes.resize_with(SEGMENT_SIZE * (additional_segments + 1), Default::default);
        controller.read_segments_into(page_id + 1, &mut bytes[SEGMENT_SIZE..])?;
        Ok(Page { page_id, bytes })
    }

    fn total_required_size(data: usize, references: usize) -> (usize, bool, bool) {
        let mut size = data + EntryID::SERIALIZED_SIZE * references;
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
        let position_and_references = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
        let position = position_and_references >> 4;
        let references = position_and_references & 15;
        let size = u8::from_le_bytes(bytes[2..].try_into().unwrap()) as usize;
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
        u16::from_le_bytes(self.bytes[3..5].try_into().unwrap()) as usize
    }

    fn num_entries(&self) -> usize {
        (self.entry_pointer_end() - 7) / 3
    }

    fn unpack_entry(&self, entry: usize) -> Option<(usize, usize, usize)> {
        if entry < self.num_entries() {
            let entry_start = 7 + 3 * entry;
            let packed_entry = self.bytes[entry_start..entry_start + 3].try_into().unwrap();
            let (mut offset, mut size, mut references) = Self::from_entry(packed_entry);
            if size == 255 {
                size =
                    u16::from_le_bytes(self.bytes[offset..offset + 2].try_into().unwrap()) as usize;
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
                references =
                    u64::from_le_bytes(self.bytes[offset..offset + 8].try_into().unwrap()) as usize;
                offset += 8;
            }
            Some((offset, size, references))
        } else {
            None
        }
    }

    fn get_entry_byte_range(&self, entry: usize) -> Option<ByteRange> {
        self.unpack_entry(entry).map(|(offset, size, references)| {
            let bytes = &self.bytes[offset..offset + size + EntryID::SERIALIZED_SIZE * references];
            ByteRange::from_bytes(bytes, references)
        })
    }

    fn used_space_start(&self) -> usize {
        if self.num_entries() == 0 {
            self.bytes.len()
        } else {
            u16::from_le_bytes(self.bytes[5..7].try_into().unwrap()) as usize
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
                tuple: EntryID::with_page_and_slot(self.page_id, 0),
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
                u16::from_le_bytes(entry_pointer_end_bytes[..2].try_into().unwrap()) as usize;
            let used_space_start =
                u16::from_le_bytes(used_space_start_bytes[..2].try_into().unwrap()) as usize;
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
                    tuple: EntryID::with_page_and_slot(
                        self.page_id,
                        (entry_pointer_end as usize - 7) / 3,
                    ),
                    data_length: length + EntryID::SERIALIZED_SIZE * references,
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

/// A EntryID that is equiped with a type for convenient deserialization into the type.
pub struct TypedEntryID<T: crate::serde::Deserialize> {
    tuple: EntryID,
    _marker: std::marker::PhantomData<T>,
}

#[cfg(test)]
mod tests {
    use super::controller::NaivePageController;
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
                .get_entry_bytes(EntryID::with_page_and_slot(1, 0))
                .unwrap();
            let bytes = entry_bytes.data_bytes();
            let entry = usize::from_le_bytes(bytes[..8].try_into().unwrap());

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
                .get_entry_bytes(EntryID::with_page_and_slot(1, 0))
                .unwrap();
            let bytes = entry_bytes.data_bytes();
            let entry = usize::from_le_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(entry, 5);
            let entry_bytes = file
                .get_entry_bytes(EntryID::with_page_and_slot(1, 1))
                .unwrap();
            let bytes = entry_bytes.data_bytes();
            let entry = usize::from_le_bytes(bytes[..8].try_into().unwrap());
            assert_eq!(entry, 900);
            let entry_bytes = file
                .get_entry_bytes(EntryID::with_page_and_slot(1, 2))
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
                .get_entry_bytes(EntryID::with_page_and_slot(1, 0))
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
                .add_reference(EntryID::with_page_and_slot(1, 1))
                .unwrap();
            guard.commit();
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(EntryID::with_page_and_slot(1, 0))
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
                    .add_reference(EntryID::with_page_and_slot(1, 1))
                    .unwrap();
                guard.commit();
            }
            let mut guard = file.reserve_space(1, 0).unwrap();
            guard
                .add_reference(EntryID::with_page_and_slot(1, 2))
                .unwrap();
        }
        {
            let mut file = NaivePageController::from_existing(Cursor::new(&mut bytes)).unwrap();
            let entry_bytes = file
                .get_entry_bytes(EntryID::with_page_and_slot(1, 0))
                .unwrap();
            let entry = entry_bytes.reference(0).unwrap();
            assert_eq!(entry.page, 1);
            assert_eq!(entry.slot, 1);
            let entry_bytes = file
                .get_entry_bytes(EntryID::with_page_and_slot(1, 1))
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
                .get_entry_bytes(EntryID::with_page_and_slot(1, 0))
                .unwrap();
            let entry = std::str::from_utf8(entry_bytes.data_bytes()).unwrap();
            assert_eq!(entry.len(), 30000);
            assert_eq!(entry, "lol".repeat(10000));
            let entry_bytes = file
                .get_entry_bytes(EntryID::with_page_and_slot(1, 1))
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
                .get_entry_bytes(EntryID::with_page_and_slot(1, 0))
                .unwrap();
            let name_len_bytes = &entry_bytes.data_bytes()[..8];
            let name_len = usize::from_le_bytes(name_len_bytes[..8].try_into().unwrap());
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
}
