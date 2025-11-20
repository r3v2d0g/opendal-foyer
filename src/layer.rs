use std::{fmt::Debug, hash::BuildHasher};

use foyer::{DefaultHasher, HybridCache};
use opendal::raw::{Access, Layer};

use crate::{
    FoyerAccess,
    access::{Block, Key},
};

/// The default size of blocks (4MiB).
///
/// When caching reads, files are split into blocks, which are then stored into and
/// fetched from [`foyer`]'s hybrid cache.
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// A [`Layer`] which allows caching reads to an underlying storage using
/// [`foyer`]'s hybrid cache.
pub struct FoyerLayer<S = DefaultHasher>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    /// The size of the blocks cached within [`foyer`]'s hybrid cache.
    ///
    /// There can only be up to [`u16::MAX`] blocks for each file.
    ///
    /// Changing this value requires creating a new empty cache.
    block_size: usize,

    /// The hybrid cache used to cache reads to the underlying storage.
    cache: HybridCache<Key, Block, S>,
}

impl<S> FoyerLayer<S>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    pub fn new(cache: HybridCache<Key, Block, S>) -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            cache,
        }
    }

    /// Changes the block size used to cache reads.
    ///
    /// When caching reads, files are split into blocks, which are then stored into and
    /// fetched from [`foyer`]'s hybrid cache.
    ///
    /// This means that the block size is the minimum number of bytes fetched from the
    /// cache for all but the last block which are part of the same file.
    ///
    /// If using an already existing hybrid cache, and unless the cache is empty, this
    /// value must be set to the same value as when the [`FoyerLayer`] was first
    /// created.
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }
}

impl<A: Access, S> Layer<A> for FoyerLayer<S>
where
    S: BuildHasher + Debug + Send + Sync + 'static,
{
    type LayeredAccess = FoyerAccess<A, S>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        FoyerAccess::new(inner, self.block_size, self.cache.clone())
    }
}
