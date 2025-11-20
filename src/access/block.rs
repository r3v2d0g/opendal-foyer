use std::{
    io::{Read, Write},
    sync::Arc,
};

use foyer::{Code, CodeResult};
use opendal::Buffer;
use serde::{Deserialize, Serialize};

/// The key of a block stored in [`foyer`]'s hybrid cache.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key {
    /// The name of the backend (e.g. the bucket name if using S3).
    ///
    /// This is necessary to make sure that if using the same hybrid cache for more than
    /// one storage backend, we don't end up with reading the wrong data if a path
    /// exists in more of those.
    pub(super) backend: Arc<str>,

    /// The root path of the backend.
    ///
    /// This is necessary to make sure that if using the same hybrid cache for more than
    /// one storage backend, we don't end up with reading the wrong data if a path
    /// exists in more of those.
    pub(super) root: Arc<str>,

    /// The path of the file of which the block is part of.
    pub(super) path: Arc<str>,

    /// The index of the block within the file.
    ///
    /// The size of the blocks is chosen when building the [`FoyerLayer`][1] and
    /// determines the offset of the block, in bytes, within the file.
    ///
    /// [1]: crate::FoyerLayer
    pub(super) index: u16,
}

/// A block stored in [`foyer`]'s hybrid cache.
pub struct Block {
    pub(super) data: Buffer,
}

impl Code for Block {
    #[inline]
    fn encode(&self, writer: &mut impl Write) -> CodeResult<()> {
        self.data.len().encode(writer)?;

        // `self.data` is cheap to clone – we are iterating over the non-continuous slices
        // of bytes which it contains to avoid having to copy bytes more than strictly
        // necessary.
        for bytes in self.data.clone() {
            writer.write_all(&bytes)?;
        }

        Ok(())
    }

    #[inline]
    fn decode(reader: &mut impl Read) -> CodeResult<Self>
    where
        Self: Sized,
    {
        let len = usize::decode(reader)?;
        let mut data = Vec::with_capacity(len);

        #[allow(clippy::uninit_vec)] // we are not reading from the `Vec`
        unsafe {
            // SAFETY: this is used in the implementation of `Code` for `String`
            data.set_len(len);
        }

        reader.read_exact(&mut data)?;

        Ok(Self { data: data.into() })
    }

    #[inline]
    fn estimated_size(&self) -> usize {
        self.data.len()
    }
}
