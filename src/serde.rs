//! Contains definitions of serialization/deserialization traits and implementations for primtives
//! and std types.

use std::convert::{TryFrom, TryInto};
use std::rc::{Rc, Weak};

use crate::controller::{NaivePageController, SegmentController};
use crate::error::{Result, SlottedPageError};
use crate::{ByteRange, EntryID, Guard, TypedEntryID};

use anyhow::anyhow;

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

    /// Serializes all the relevant references. The EntryIDs should be stored in the references
    /// vector. The references will be stored in the order given in the vector.
    fn serialize_references<'a, S: SegmentController>(
        &self,
        file: &'a mut NaivePageController<S>,
        references: &mut Vec<EntryID>,
    ) -> Result<()>;

    /// Serializes the data part of the object. The guard passed in will have at least data_length()
    /// data bytes free.  
    fn serialize_data(&self, guard: &mut Guard) -> Result<()>;

    /// Serializes the object to file.
    fn serialize<S: SegmentController>(
        &self,
        file: &mut NaivePageController<S>,
    ) -> Result<EntryID> {
        // Behaviour of serialize
        // 1) Calculate the total structure size by summing each field's size.
        // 2) Loop through each field and serialize references to EntryIDs.
        // 3) Get a guard for the total size.
        // 4) Add all the references to the guard.
        // 5) Add all the data.
        // 6) Commit the guard to get a resultant EntryID.
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

/// A trait for items that can be deserialized. The difference between Deserialize and Loadable is
/// that Deserialize takes in an additional entry ID to deserialize, while Loadable does not.
pub trait Deserialize: Sized {
    /// The LoadType is the proto representation of the Self, but one that can be loaded into Self.
    /// The LoadType cannot borrow items from the ByteRange.
    type LoadType: Loadable<LoadType = Self> + for<'a> TryFrom<ByteRange<'a>, Error = anyhow::Error>;

    /// Deserializes the given entry ID into an object.
    fn deserialize<S: SegmentController>(
        file: &mut NaivePageController<S>,
        tuple: EntryID,
    ) -> Result<Self> {
        let x: Self::LoadType = file
            .get_entry_bytes(tuple)?
            .try_into()
            .map_err(|x: anyhow::Error| SlottedPageError::DeserializationError(x.into()))?;
        x.load(file)
    }
}

/// A wrapper type to indicate that loading is not necessary for a type. Objects wrapped in this
/// type will be return verbatim when loading.
pub struct LoadNotNecessary<T: for<'a> TryFrom<ByteRange<'a>, Error = anyhow::Error>>(T);

pub(crate) mod r#impl {
    use super::*;

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

    impl<T: Deserialize> Loadable for TypedEntryID<T> {
        type LoadType = T;

        fn load<S: SegmentController>(
            self,
            file: &mut NaivePageController<S>,
        ) -> Result<Self::LoadType> {
            T::deserialize(file, self.tuple)
        }
    }

    impl Serialize for bool {
        fn data_length(&self) -> usize {
            1
        }

        fn serialize_references<'a, S: SegmentController>(
            &self,
            _file: &'a mut NaivePageController<S>,
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            _references: &mut Vec<EntryID>,
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
            references: &mut Vec<EntryID>,
        ) -> Result<()> {
            references.push(self.as_ref().serialize(file)?);
            Ok(())
        }

        fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
            Ok(())
        }
    }

    impl<'a, T: Deserialize> TryFrom<ByteRange<'a>> for TypedEntryID<T> {
        type Error = anyhow::Error;

        fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
            let reference = value.reference(0).unwrap();
            Ok(TypedEntryID {
                tuple: reference,
                _marker: std::marker::PhantomData,
            })
        }
    }

    /// A wrapper type to load an item into a Box.
    pub struct BoxedEntryID<T: Deserialize>(TypedEntryID<T>);

    impl<'a, T: Deserialize> TryFrom<ByteRange<'a>> for BoxedEntryID<T> {
        type Error = anyhow::Error;

        fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
            let reference = value.reference(0).unwrap();
            Ok(BoxedEntryID(TypedEntryID {
                tuple: reference,
                _marker: std::marker::PhantomData,
            }))
        }
    }

    impl<T: Deserialize> Loadable for BoxedEntryID<T> {
        type LoadType = Box<T>;

        fn load<S: SegmentController>(
            self,
            file: &mut NaivePageController<S>,
        ) -> Result<Self::LoadType> {
            Ok(Box::new(self.0.load(file)?))
        }
    }

    impl<T: Deserialize> Deserialize for Box<T> {
        type LoadType = BoxedEntryID<T>;
    }

    /// A wrapper type to load an item into a Rc.
    pub struct RcedEntryID<T: Deserialize>(TypedEntryID<T>);

    impl<'a, T: Deserialize> TryFrom<ByteRange<'a>> for RcedEntryID<T> {
        type Error = anyhow::Error;

        fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
            let reference = value.reference(0).unwrap();
            Ok(RcedEntryID(TypedEntryID {
                tuple: reference,
                _marker: std::marker::PhantomData,
            }))
        }
    }

    impl<T: Deserialize> Loadable for RcedEntryID<T> {
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
            references: &mut Vec<EntryID>,
        ) -> Result<()> {
            references.push(self.as_ref().serialize(file)?);
            Ok(())
        }

        fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
            Ok(())
        }
    }

    impl<T: Deserialize> Deserialize for Rc<T> {
        type LoadType = RcedEntryID<T>;
    }

    impl<T: Serialize> Serialize for Weak<T> {
        fn data_length(&self) -> usize {
            0
        }

        fn serialize_references<'a, S: SegmentController>(
            &self,
            file: &'a mut NaivePageController<S>,
            references: &mut Vec<EntryID>,
        ) -> Result<()> {
            references.push(self.upgrade().unwrap().as_ref().serialize(file)?);
            Ok(())
        }

        fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
            Ok(())
        }
    }

    impl Serialize for EntryID {
        fn data_length(&self) -> usize {
            0
        }

        fn serialize_references<'a, S: SegmentController>(
            &self,
            _file: &'a mut NaivePageController<S>,
            references: &mut Vec<EntryID>,
        ) -> Result<()> {
            references.push(*self);
            Ok(())
        }

        fn serialize_data(&self, _guard: &mut Guard) -> Result<()> {
            Ok(())
        }
    }

    impl<'a> TryFrom<ByteRange<'a>> for EntryID {
        type Error = anyhow::Error;

        fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
            let reference = value.reference(0).unwrap();
            Ok(reference)
        }
    }

    impl Deserialize for EntryID {
        type LoadType = LoadNotNecessary<EntryID>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;
    use std::io::Cursor;

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
        let ref_1_val = EntryID::deserialize(&mut file, ref1).unwrap();
        let ref_2_val = EntryID::deserialize(&mut file, ref2).unwrap();
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

        let reference_value = EntryID::deserialize(&mut file, reference_id).unwrap();
        assert_eq!(reference_value, large_id);

        let large_value = String::deserialize(&mut file, reference_value).unwrap();
        assert_eq!(large_value, "lol".repeat(10000));
    }

    #[test]
    fn test_complex_serialize() {
        #[derive(Debug, PartialEq, Eq)]
        struct Person {
            name: String,
            occupation: String,
        }

        impl Serialize for Person {
            fn data_length(&self) -> usize {
                8 + self.name.len() + self.occupation.len()
            }

            fn serialize_references<'a, S: SegmentController>(
                &self,
                _file: &'a mut NaivePageController<S>,
                _references: &mut Vec<EntryID>,
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
            type LoadType = crate::serde::LoadNotNecessary<Person>;
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
            name: TypedEntryID<String>,
            occupation: TypedEntryID<String>,
        }

        impl<'a> TryFrom<ByteRange<'a>> for LazyPerson {
            type Error = anyhow::Error;

            fn try_from(value: ByteRange<'a>) -> std::result::Result<Self, Self::Error> {
                let name = value.reference(0).unwrap();
                let occupation = value.reference(1).unwrap();
                Ok(LazyPerson {
                    name: TypedEntryID {
                        tuple: name,
                        _marker: std::marker::PhantomData,
                    },
                    occupation: TypedEntryID {
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
                references: &mut Vec<EntryID>,
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
