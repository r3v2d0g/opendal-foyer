use foyer::Code;

use crate::access::{Block, Key};

/// A [`Weighter`] for the `foyer` cache provided to [`FoyerLayer`][1].
///
/// This returns the size of the block, in bytes.
///
/// [1]: crate::FoyerLayer
pub fn weighter(_: &Key, block: &Block) -> usize {
    block.estimated_size()
}
