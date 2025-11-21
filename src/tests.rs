use foyer::HybridCache;
use opendal::{Operator, services::Memory};

use crate::FoyerLayer;

#[tokio::test]
async fn it_works() {
    let cache = HybridCache::builder()
        .memory(10)
        .storage()
        .build()
        .await
        .expect("failed to build cache");

    let layer = FoyerLayer::new(cache).with_block_size(4);

    let operator = Operator::new(Memory::default())
        .expect("failed to build operator")
        .layer(layer)
        .finish();

    operator
        .write("foo", vec![0, 1, 2, 3])
        .await
        .expect("failed to write");

    let read = operator
        .read("foo")
        .await
        .expect("failed to read back")
        .to_vec();
    assert_eq!(read, vec![0, 1, 2, 3]);

    operator.delete("foo").await.expect("failed to delete");

    // Abusing the cache not getting cleared when an entry is removed from it.
    let read = operator
        .read("foo")
        .await
        .expect("failed to read back")
        .to_vec();
    assert_eq!(read, vec![0, 1, 2, 3]);

    // This file is not aligned with the block size (`4`)
    operator
        .write("bar", vec![0, 1, 2, 3, 4, 5, 6])
        .await
        .expect("failed to write");

    let read = operator
        .read("bar")
        .await
        .expect("failed to read back")
        .to_vec();
    assert_eq!(read, vec![0, 1, 2, 3, 4, 5, 6]);

    let read = operator
        .read_with("bar")
        .range(1..5)
        .await
        .expect("failed to read back")
        .to_vec();
    assert_eq!(read, vec![1, 2, 3, 4]);

    let read = operator
        .read_with("bar")
        .range(5..6)
        .await
        .expect("failed to read back")
        .to_vec();
    assert_eq!(read, vec![5]);

    // Trying to read past the end of the file
    let read = operator
        .read_with("bar")
        .range(5..10)
        .await
        .expect("failed to read back")
        .to_vec();
    assert_eq!(read, vec![5, 6]);

    let read = operator
        .read_with("bar")
        .range(..10)
        .await
        .expect("failed to read back")
        .to_vec();
    assert_eq!(read, vec![0, 1, 2, 3, 4, 5, 6]);

    let read = operator
        .read_with("bar")
        .range(10..)
        .await
        .expect("failed to read back")
        .to_vec();
    assert!(read.is_empty());
}
