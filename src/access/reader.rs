use std::{fmt::Debug, hash::BuildHasher, ops::Range, sync::Arc};

use foyer::{DefaultHasher, HybridCache};
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
    /// The underlying storage whose reads are being cached.
    inner: Arc<A>,

    /// The size of the blocks cached within [`foyer`]'s hybrid cache.
    ///
    /// There can only be up to [`u16::MAX`] blocks for each file.
    ///
    /// Changing this value requires creating a new empty cache.
    block_size: usize,

    /// The hybrid cache used to cache reads to the underlying storage.
    cache: HybridCache<Key, Block, S>,

    /// The blocks to read, as well as the range of bytes to include within each block.
    blocks: Vec<(Range<usize>, Key)>,

    /// The index of the next block to read within `blocks`.
    next: usize,
}

impl<A: Access, S> Reader<A, S>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    pub(super) fn new(
        inner: Arc<A>,
        block_size: usize,
        cache: HybridCache<Key, Block, S>,
        blocks: Vec<(Range<usize>, Key)>,
    ) -> Self {
        Self {
            inner,
            block_size,
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

        let (range, key) = self.blocks[current].clone();
        if let Some(block) = self
            .cache
            .obtain(key.clone())
            .await
            .map_err(|error| Error::new(ErrorKind::Unexpected, error.to_string()))?
        {
            let buffer = block.data.slice(range);
            return Ok(buffer);
        }

        let path = key.path.as_ref();
        let offset = self.block_size * key.index as usize + range.start;
        let size = range.end - range.start;
        let range = BytesRange::new(offset as u64, Some(size as u64));
        let args = OpRead::new().with_range(range);

        let (_, mut reader) = self.inner.read(path, args).await?;
        let buffer = reader.read_all().await?;

        let block = Block {
            data: buffer.clone(),
        };

        self.cache.insert(key, block);

        Ok(buffer)
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
