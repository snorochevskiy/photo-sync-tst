mod common;

use std::sync::Arc;

use anyhow::Result;
use photo_sync_tst::catalog::{CatalogNode, RemotePeer};

#[test]
fn test_synchronization() -> Result<()> {
    // Given two peers
    let peer1: Arc<CatalogNode> = Arc::new(CatalogNode::test_new("s1")?);
    let peer2 = Arc::new(CatalogNode::test_new("s2")?);

    peer1.add_peer(peer2.clone());
    peer2.add_peer(peer1.clone());

    // Adding photo object IDs to firsts
    peer1.propose(20210711, &vec![(img!(0), peers!(0))])?;

    // Verify second peer doesn't know about newly added photos yet
    assert_eq!(0, peer2.get_years_checksums()?.len());

    // Synchronizing peers
    peer1.sync_with_peers()?;

    // The second peer is aware of photos from first peer
    assert_eq!(1, peer2.get_years_checksums()?.len());
    assert_eq!(Some(vec![(img!(0), peers!(0))]), peer2.get_data(20210711)?);

    // Now adding a photo to the second peer
    peer2.propose(20210711, &vec![(img!(1), peers!(0))])?;

    // Launching sync on first peer
    peer1.sync_with_peers()?;

    // Verify updates have been fetched from peer 2
    assert_eq!(
        Some(vec![(img!(0), peers!(0)), (img!(1), peers!(0))]),
        peer1.get_data(20210711)?
    );

    Ok(())
}
