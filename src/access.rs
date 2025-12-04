use std::{cmp, fmt::Debug, hash::BuildHasher, sync::Arc};

use foyer::{DefaultHasher, HybridCache};
use futures::prelude::*;
use opendal::{
    Metadata, Result,
    raw::{
        Access, BytesRange, LayeredAccess, MaybeSend, OpCopy, OpList, OpPresign, OpRead, OpRename,
        OpStat, OpWrite, RpCopy, RpDelete, RpList, RpPresign, RpRead, RpRename, RpStat, RpWrite,
    },
};
use scc::HashMap;

use crate::access::reader::ReadBlock;

pub(crate) use self::block::{Block, Key};
use self::reader::Reader;

mod block;
mod reader;

/// An implementation of [`LayeredAccess`] which caches reads using [`foyer`]'s
/// hybrid cache.
#[derive(Debug)]
pub struct FoyerAccess<A, S = DefaultHasher>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    /// The underlying storage this is caching the reads of.
    inner: Arc<A>,

    /// The size of the blocks cached within [`foyer`]'s hybrid cache.
    ///
    /// There can only be up to [`u16::MAX`] blocks for each file.
    ///
    /// Changing this value requires creating a new empty cache.
    block_size: usize,

    /// The hybrid cache used to cache reads to the underlying storage.
    cache: HybridCache<Key, Block, S>,

    /// Caches the metadata, which is used by both the `stat()` operation and the
    /// `read()` one to know the full size of a file.
    // TODO(MLB): remove from the cache eventually?
    metadata: HashMap<Arc<str>, Metadata>,
}

impl<A: Access, S> FoyerAccess<A, S>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    pub(crate) fn new(inner: A, block_size: usize, cache: HybridCache<Key, Block, S>) -> Self {
        let inner = Arc::new(inner);
        inner.info().update_full_capability(|mut cap| {
            cap.stat_with_if_match = false;
            cap.stat_with_if_none_match = false;
            cap.stat_with_if_modified_since = false;
            cap.stat_with_if_unmodified_since = false;
            cap.stat_with_override_content_type = false;
            cap.stat_with_override_cache_control = false;
            cap.stat_with_override_content_disposition = false;
            cap.stat_with_version = false;

            cap.read_with_if_match = false;
            cap.read_with_if_none_match = false;
            cap.read_with_if_modified_since = false;
            cap.read_with_if_unmodified_since = false;
            cap.read_with_override_cache_control = false;
            cap.read_with_override_content_disposition = false;
            cap.read_with_override_content_type = false;
            cap.read_with_version = false;
            cap
        });

        Self {
            inner,
            block_size,
            cache,
            metadata: HashMap::default(),
        }
    }

    /// Fetches the metadata for the given path.
    ///
    /// This first looks at whether the metadata is already cached. If not, it fetches
    /// it from the underlying storage and then adds it to the cache.
    async fn metadata(&self, path: Arc<str>) -> Result<Metadata> {
        if let Some(metadata) = self
            .metadata
            .read_async(&path, |_, metadata| metadata.clone())
            .await
        {
            return Ok(metadata);
        }

        let args = OpStat::new();
        let stat = self.inner.stat(path.as_ref(), args).await?;
        let metadata = stat.into_metadata();

        self.metadata.upsert_async(path, metadata.clone()).await;

        Ok(metadata)
    }

    /// Returns the list of [`Key`] of the blocks to fetch for the given path and range.
    ///
    /// Depending on the `block_size` value and the provided `range`, this computes the
    /// blocks that have to be fetched.
    async fn blocks(&self, path: &str, range: BytesRange) -> Result<Vec<ReadBlock>> {
        let info = self.inner.info();

        let backend = info.name();
        let root = info.root();
        let path = Arc::from(path);

        let offset = range.offset() as usize;

        let metadata = self.metadata(Arc::clone(&path)).await?;
        let file_size = metadata.content_length() as usize;

        if offset >= file_size {
            return Ok(vec![]);
        }

        let size = if let Some(size) = range.size() {
            cmp::min(size as usize, file_size - offset)
        } else {
            file_size - offset
        };

        let mut num_blocks = size / self.block_size;

        // If the number of bytes we're reading is not aligned, we'll have to read some
        // bytes at the beginning of a block.
        if !size.is_multiple_of(self.block_size) {
            num_blocks += 1;
        }

        // If the offset is not aligned, we'll have to read some bytes at the end of a
        // block.
        if !offset.is_multiple_of(self.block_size) {
            num_blocks += 1;
        }

        let mut blocks = Vec::with_capacity(num_blocks);
        let mut remaining = size;
        // TODO(MLB): pre-validate that we do not go over `u16::MAX`
        let mut index = (offset / self.block_size) as u16;

        while remaining > 0 {
            // If this is the first block to read, we skip the first `offset` bytes.
            let start = if remaining == size {
                offset - index as usize * self.block_size
            } else {
                0
            };

            // If this is the last block to read, we only take the `remaining` bytes.
            let (end, size) = if remaining <= (self.block_size - start) {
                let end = start + remaining;

                // If this is the last in the file, we make sure not to try to read past
                // `file_size`.
                let block_end = index as usize * self.block_size;
                let size = cmp::min(self.block_size, file_size - block_end);

                (end, size)
            } else {
                (self.block_size, self.block_size)
            };

            let range = start..end;
            let key = Key {
                backend: Arc::clone(&backend),
                root: Arc::clone(&root),
                path: Arc::clone(&path),
                index,
            };

            blocks.push(ReadBlock { key, range, size });

            remaining = remaining.saturating_sub(end - start);
            index += 1;
        }

        Ok(blocks)
    }
}

impl<A: Access, S: 'static> LayeredAccess for FoyerAccess<A, S>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    type Inner = A;
    type Reader = Reader<A, S>;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    #[inline]
    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    #[inline]
    fn stat(&self, path: &str, _: OpStat) -> impl Future<Output = Result<RpStat>> + MaybeSend {
        let path = Arc::from(path);
        self.metadata(path).map_ok(RpStat::new)
    }

    #[inline]
    fn read(
        &self,
        path: &str,
        args: OpRead,
    ) -> impl Future<Output = Result<(RpRead, Self::Reader)>> + MaybeSend {
        let range = args.range();
        self.blocks(path, range).map_ok(|blocks| {
            let reply = RpRead::new();
            // TODO(MLB): `with_size` and `with_range`

            let inner = Arc::clone(&self.inner);
            let cache = self.cache.clone();
            let reader = Reader::new(inner, self.block_size, cache, blocks);

            (reply, reader)
        })
    }

    #[inline]
    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        self.inner.write(path, args)
    }

    #[inline]
    fn delete(&self) -> impl Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        self.inner.delete()
    }

    #[inline]
    fn list(
        &self,
        path: &str,
        args: OpList,
    ) -> impl Future<Output = Result<(RpList, Self::Lister)>> + MaybeSend {
        self.inner.list(path, args)
    }

    #[inline]
    fn copy(
        &self,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> impl Future<Output = Result<RpCopy>> + MaybeSend {
        self.inner.copy(from, to, args)
    }

    #[inline]
    fn rename(
        &self,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> impl Future<Output = Result<RpRename>> + MaybeSend {
        self.inner.rename(from, to, args)
    }

    #[inline]
    fn presign(
        &self,
        path: &str,
        args: OpPresign,
    ) -> impl Future<Output = Result<RpPresign>> + MaybeSend {
        self.inner.presign(path, args)
    }
}
