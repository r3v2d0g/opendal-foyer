use std::{fmt::Debug, hash::BuildHasher, ops::Range, sync::Arc};

use foyer::{DefaultHasher, HybridCache};
use futures::prelude::*;
use opendal::{
    Buffer, Error, ErrorKind, Result,
    raw::{Access, BytesRange, MaybeSend, OpRead, oio::Read},
};

use super::{Block, Key};

/// A reader reading a bunch of potentially cached blocks.
///
/// For blocks which aren't already cached, this adds them to [`foyer`]'s hybrid
/// cache.
///
/// Blocks which are already cached are returned as-is.
pub struct Reader<A, S = DefaultHasher>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    /// The backend storage used to read blocks.
    backend: Backend<A>,

    /// The hybrid cache used to cache reads to the underlying storage.
    cache: HybridCache<Key, Block, S>,

    /// The blocks to read.
    blocks: Vec<ReadBlock>,

    /// The index of the next block to read within `blocks`.
    next: usize,
}

/// The backend storage of a [`Reader`].
struct Backend<A> {
    /// The underlying storage whose reads are being cached.
    inner: Arc<A>,

    /// The size of the blocks cached within [`foyer`]'s hybrid cache.
    ///
    /// There can only be up to [`u16::MAX`] blocks for each file.
    ///
    /// Changing this value requires creating a new empty cache.
    block_size: usize,
}

/// A block to read as part of a [`Reader`].
#[derive(Clone, Debug)]
pub(super) struct ReadBlock {
    /// The key for the block, used to fetch it from the cache, or to put it into the
    /// cache.
    pub key: Key,

    /// The range of bytes to read within the block.
    pub range: Range<usize>,

    /// The size of the block, if it needs to be fetched from the storage backend.
    pub size: usize,
}

impl<A: Access, S> Reader<A, S>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    pub(super) fn new(
        inner: Arc<A>,
        block_size: usize,
        cache: HybridCache<Key, Block, S>,
        blocks: Vec<ReadBlock>,
    ) -> Self {
        let backend = Backend { inner, block_size };

        Self {
            backend,
            cache,
            blocks,
            next: 0,
        }
    }

    /// Fetches the [`Buffer`] for the next block.
    ///
    /// This first looks at whether the block is already cached. If not, it reads it
    /// from the underlying storage and then adds it to the cache.
    async fn next(&mut self) -> Result<Buffer> {
        let current = self.next;
        if current == self.blocks.len() {
            return Ok(Buffer::new());
        }

        self.next = current + 1;

        let ReadBlock { key, range, size } = self.blocks[current].clone();
        let block = self
            .cache
            .get_or_fetch(&key.clone(), || {
                self.backend
                    .clone()
                    .read(key, size)
                    .map_err(|error| foyer::Error::io_error(error.into()))
            })
            .map_err(|error| Error::new(ErrorKind::Unexpected, error.to_string()))
            .await?;

        let buffer = block.data.slice(range);
        Ok(buffer)
    }
}

impl<A: Access> Backend<A> {
    /// Reads the block with the given key from the storage backend.
    ///
    /// This does not take a reference to make it easy to read from within a call to
    /// [`get_or_fetch()`][1].
    ///
    /// [1]: HybridCache::get_or_fetch()
    async fn read(self, key: Key, size: usize) -> Result<Block> {
        let path = key.path.as_ref();
        let offset = self.block_size * key.index as usize;
        let range = BytesRange::new(offset as u64, Some(size as u64));
        let args = OpRead::new().with_range(range);

        let (_, mut reader) = self.inner.read(path, args).await?;
        let data = reader.read_all().await?;

        Ok(Block { data })
    }
}

impl<A: Access, S> Read for Reader<A, S>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    fn read(&mut self) -> impl Future<Output = Result<Buffer>> + MaybeSend {
        self.next()
    }
}

impl<A> Clone for Backend<A> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            block_size: self.block_size,
        }
    }
}
