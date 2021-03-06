//! Contains definitions of core controller traits and the core implementations.

use std::io::SeekFrom;

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{Result, SlottedPageError};
use crate::{ByteRange, EntryID, FileHeader, Guard, Page, SEGMENT_SIZE};

/// A SegmentController is just an extension on Write, Read and Seek types to load segments from
/// files.
#[async_trait]
pub trait SegmentController: Send {
    /// Read multiple segments from the source. If bytes is not a multiple of a segment size the
    /// remainder of the buffer will not be written to.
    async fn read_segments_into(&mut self, start_segment_id: usize, bytes: &mut [u8])
        -> Result<()>;

    /// A helper function to load a single segment into a fixed array.
    async fn read_segment(&mut self, segment_id: usize) -> Result<[u8; SEGMENT_SIZE]> {
        let mut bytes = [0; SEGMENT_SIZE];
        self.read_segments_into(segment_id, &mut bytes).await?;
        Ok(bytes)
    }

    /// Writes multiple segments into the source at the given start segment. Only whole segments
    /// will be written to the sink.
    async fn write_segments(&mut self, start_segment_id: usize, bytes: &[u8]) -> Result<()>;
}

/// Implements the helper code for Write + Read + Seek types.
#[async_trait]
impl<T> SegmentController for T
where
    T: AsyncWriteExt + AsyncReadExt + AsyncSeekExt + Send + Unpin,
{
    async fn read_segments_into(&mut self, segment_id: usize, bytes: &mut [u8]) -> Result<()> {
        let offset = segment_id * SEGMENT_SIZE;
        let length = (bytes.len() / SEGMENT_SIZE) * SEGMENT_SIZE;
        let bytes = &mut bytes[..length];
        self.seek(SeekFrom::Start(offset as u64)).await?;
        self.read_exact(bytes).await?;
        Ok(())
    }

    async fn write_segments(&mut self, start_segment_id: usize, bytes: &[u8]) -> Result<()> {
        let offset = start_segment_id * SEGMENT_SIZE;
        let length = (bytes.len() / SEGMENT_SIZE) * SEGMENT_SIZE;
        let bytes = &bytes[..length];
        self.seek(SeekFrom::Start(offset as u64)).await?;
        self.write_all(bytes).await.map_err(SlottedPageError::from)
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
    pub async fn from_existing(mut segment_controller: S) -> Result<Self> {
        let header = FileHeader::from_bytes(segment_controller.read_segment(0).await?)?;
        Ok(NaivePageController {
            header,
            header_is_dirty: false,
            segment_controller,
            current_page: None,
        })
    }

    async fn load_page(&mut self, page_id: usize) -> Result<()> {
        if let Some((_, old_page)) = &self.current_page {
            if old_page.page_id() != page_id {
                self.save_current_page(false).await?;
                self.current_page = Some((
                    false,
                    Page::from_bytes(page_id, &mut self.segment_controller).await?,
                ));
            }
        } else {
            self.current_page = Some((
                false,
                Page::from_bytes(page_id, &mut self.segment_controller).await?,
            ));
        }
        Ok(())
    }

    async fn save_current_page(&mut self, force: bool) -> Result<()> {
        if let Some((dirty, old_page)) = &mut self.current_page {
            if *dirty || force {
                self.segment_controller
                    .write_segments(old_page.page_id(), old_page.to_bytes().as_ref())
                    .await?;
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
    pub async fn get_entry_bytes(&mut self, tuple: EntryID) -> Result<ByteRange<'_>> {
        self.load_page(tuple.page).await?;
        let page = self.current_page.as_ref().unwrap();
        page.1
            .get_entry_byte_range(tuple.slot)
            .ok_or_else(|| SlottedPageError::NotFound(tuple))
    }

    /// Reserves space for an item to be written. The length must be given in the number of
    /// references and the number of data bytes required. Additionally, this function can be used to
    /// set the root node of the file.
    pub fn reserve_space(
        &'a mut self,
        references: usize,
        data_length: usize,
        set_root: bool,
    ) -> Result<Guard> {
        let length = references * EntryID::SERIALIZED_SIZE + data_length;
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
        let file_header = if set_root {
            Some(&mut self.header)
        } else {
            None
        };
        Ok(self
            .current_page
            .as_mut()
            .unwrap()
            .1
            .reserve_space(references, data_length, file_header)
            .unwrap())
    }

    /// Flushes the outstanding stale contents to disk.
    pub async fn flush(&mut self) -> Result<()> {
        if self.header_is_dirty {
            self.segment_controller
                .write_segments(0, &self.header.to_bytes())
                .await?;
        }
        self.save_current_page(false).await
    }
}
