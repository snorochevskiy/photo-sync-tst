use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use crate::local_storage::Checksum;
use crate::local_storage::Data;
use crate::local_storage::LocalStorage;
use crate::local_storage::Peer;
use crate::opaque_date::ymd_interval_for_y;
use crate::opaque_date::ymd_interval_for_ym;
use crate::opaque_date::Year;
use crate::opaque_date::YearMonth;
use crate::opaque_date::YearMonthDay;
use anyhow::Result;
use itertools::Itertools;
use thiserror::Error;

use log::debug;

#[derive(Error, Debug)]
pub enum DistStoreError {
    #[error("The syncronization is already in process")]
    SyncInProcess,
}

/// Represents a remote peer we can exchange photos with.
/// It is assumed that in real system, this trait should be implemented
/// using a network client, that communicates with another instance
/// of [`DistributedObjStorage`](DistributedObjStorage).
pub trait RemotePeer {
    /// Returns ID of the peer. The ID should not change between session of connection to peer.
    fn id(&self) -> Vec<u8>;

    /// Callback for a peer to notified that it has been added by calling peer.
    fn notify_added_by(&self, peer: Arc<dyn RemotePeer>);

    /// Return all year partitions along with treir checksum.
    /// If thecksum of a remote partition is same as the checksum for same partition locally,
    /// then no synchonization for given year is required.
    fn get_years_checksums(&self) -> Result<Vec<(Year, Checksum)>>;

    /// Return list of months with checksums for given year.
    fn get_months_checksum(&self, y: Year) -> Result<Vec<(YearMonth, Checksum)>>;

    /// Return list of days with checksums for given year/month.
    fn get_days_checksum(&self, ym: YearMonth) -> Result<Vec<(YearMonthDay, Checksum)>>;

    /// For given interval, return list of year/month/day records that exist in the storage,
    /// i.e. for these days there are object IDs stored.
    fn get_existing_days_in_range(
        &self,
        ymd_from: YearMonthDay,
        ymd_to: YearMonthDay,
    ) -> Result<Vec<YearMonthDay>>;

    /// Return object IDs for given day.
    /// Each object ID is associated with a list of peers that have the object on their host.
    fn get_data(&self, ymd: u32) -> Result<Option<Vec<(Data, Vec<Peer>)>>>;

    /// Propose list of object IDs for given day to the peer.
    fn propose(&self, ymd: u32, data: &[(Data, Vec<Peer>)]) -> Result<Vec<u8>>;
}

/// Represents a local instance of a distributed object IDs storage.
/// It keeps a list of object IDs partitioned by year, month and day
/// and can synchronize this list with other peers.
pub struct CatalogNode {
    name: String,
    storage: LocalStorage,
    peers: RwLock<Vec<Arc<dyn RemotePeer>>>,
    sync_mutex: Mutex<()>,
}

impl CatalogNode {
    pub fn new<S: Into<String>, P: AsRef<Path>>(name: S, path: P) -> Result<CatalogNode> {
        Ok(CatalogNode {
            name: name.into(),
            storage: LocalStorage::new(path)?,
            peers: RwLock::new(Vec::new()),
            sync_mutex: Mutex::new(()),
        })
    }

    pub fn test_new(name: &str) -> Result<CatalogNode> {
        Ok(CatalogNode {
            name: name.into(),
            storage: LocalStorage::test_new()?,
            peers: RwLock::new(Vec::new()),
            sync_mutex: Mutex::new(()),
        })
    }

    /// Adding a peer.
    /// In real system a disconnection of a peer should be handled,
    /// but it is out of the scope of this task.
    pub fn add_peer(&self, peer: Arc<dyn RemotePeer>) {
        {
            // It only one level of locking, safe to unwrap
            let mut guard = self.peers.write().unwrap();
            guard.push(peer);
        }
    }

    pub fn id(&self) -> Vec<u8> {
        self.name.as_bytes().to_vec()
    }

    /// Performs the synchronization with all know peers.
    /// To do that it compares checksums for years, then year/months and year/month/days.
    /// Data for days are have different checksums is synchronized between peers.
    /// Checksums are recalculated after the syncronization.
    pub fn sync_with_peers(&self) -> Result<()> {
        let _guard = match self.sync_mutex.try_lock() {
            Ok(guard) => guard,
            _ => return Err(DistStoreError::SyncInProcess.into()),
        };
        debug!("Starting synchronization with peers");

        let peers: Vec<Arc<dyn RemotePeer>> = {
            let peers_guard = &self.peers.read().unwrap();
            peers_guard.deref().clone()
        };

        // Cyclomatic complexity is not great, but in this case it makes the alrorithm clearer
        for peer in peers {
            let (missing_on_local, missing_on_remote, diff_y) =
                calc_diff(&self.get_years_checksums()?, &peer.get_years_checksums()?);
            fill_gaps(peer.as_ref(), self, missing_on_local, ymd_interval_for_y)?;
            fill_gaps(self, peer.as_ref(), missing_on_remote, ymd_interval_for_y)?;

            for y in diff_y {
                let (missing_on_local, missing_on_remote, diff_ym) =
                    calc_diff(&self.get_months_checksum(y)?, &peer.get_months_checksum(y)?);
                fill_gaps(peer.as_ref(), self, missing_on_local, ymd_interval_for_ym)?;
                fill_gaps(self, peer.as_ref(), missing_on_remote, ymd_interval_for_ym)?;

                for ym in diff_ym {
                    let (mut missing_on_local, mut missing_on_remote, diff_ymd) =
                        calc_diff(&self.get_days_checksum(ym)?, &peer.get_days_checksum(ym)?);
                    missing_on_local.extend(&diff_ymd);
                    missing_on_remote.extend(&diff_ymd);
                    fill_ymd_gaps(peer.as_ref(), self, missing_on_local)?;
                    fill_ymd_gaps(self, peer.as_ref(), missing_on_remote)?;
                }
            }
        }
        debug!("Finished synchronization with peers");
        Ok(())
    }

    /// This function should be responsible for retrieving a photo file by the ID.
    /// The implementation lies out of the scope of this concept.
    pub fn retrive_photo(_ymd: YearMonthDay, _hash: Data) -> Result<Vec<u8>> {
        // 1 - Check locally
        // 2 - Check known peers
        // 3 - If known peers now available, check for other peers
        todo!()
    }
}

/// For given dates performs the data interchange between peers.
/// Arg:
/// * src - peer we synchronizing data from
/// * dst - peer that receives data
/// * dates - the transfer should be performed for
/// * date_to_interval - function that converts given date to a renge of year/month/day partitions
///   it is required to make the sync function to be able to work with both year and year/month partitions.
fn fill_gaps(
    src: &dyn RemotePeer,
    dst: &dyn RemotePeer,
    dates: Vec<u32>,
    date_to_interval: fn(u32) -> (YearMonthDay, YearMonthDay),
) -> Result<()> {
    for d in dates {
        let (start, end) = date_to_interval(d);
        let days = src.get_existing_days_in_range(start, end)?;
        for ymd in days {
            if let Some(photos) = src.get_data(ymd)? {
                dst.propose(ymd, &photos)?;
            }
        }
    }
    Ok(())
}

/// For given year/month/day partitions performs the data interchange between peers.
fn fill_ymd_gaps(
    src: &dyn RemotePeer,
    dst: &dyn RemotePeer,
    ymds: Vec<YearMonthDay>,
) -> Result<()> {
    for ymd in ymds {
        if let Some(photos) = src.get_data(ymd)? {
            dst.propose(ymd, &photos)?;
        }
    }
    Ok(())
}

/// Takes two sorted sequences of pairs (data, checksum)
/// and returns triplet:
/// * pairs that exist in second sequence but absent in the first one
/// * pairs that exist in first sequence but absent in the second one
/// * pairs that present in both sequences, but have different checksum
fn calc_diff(
    local: &[(u32, Checksum)],
    remote: &[(u32, Checksum)],
) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
    let mut missing_on_local = Vec::<u32>::new();
    let mut missing_on_remote = Vec::<u32>::new();
    let mut different = Vec::<u32>::new();

    let mut l_ind = 0;
    let mut r_ind = 0;

    while l_ind < local.len() && r_ind < remote.len() {
        if local[l_ind].0 < remote[r_ind].0 {
            missing_on_remote.push(local[l_ind].0);
            l_ind += 1;
        } else if local[l_ind].0 > remote[r_ind].0 {
            missing_on_local.push(remote[r_ind].0);
            r_ind += 1;
        } else {
            if local[l_ind].1 != remote[r_ind].1 {
                different.push(local[l_ind].0);
            }
            l_ind += 1;
            r_ind += 1;
        }
    }

    if l_ind < local.len() {
        missing_on_remote.extend(&local[l_ind..].iter().map(|e| e.0).collect_vec());
    }
    if r_ind < remote.len() {
        missing_on_local.extend(&remote[r_ind..].iter().map(|e| e.0).collect_vec());
    }

    (missing_on_local, missing_on_remote, different)
}

/// Implementation of remote peer functionality for a local instance of catalog itself.
/// Designed for testing purposes.
/// For production, we probaly need an implementation of `RemotePeer` that interacts with peers over the network.
impl RemotePeer for CatalogNode {
    fn id(&self) -> Vec<u8> {
        self.id()
    }

    fn notify_added_by(&self, peer: Arc<dyn RemotePeer>) {
        debug!("Peer {:?}, has been added by {:?}", self.id(), peer.id());
        // Here we can make a cross reference if needed
    }

    fn get_years_checksums(&self) -> Result<Vec<(u32, Vec<u8>)>> {
        self.storage.get_years_checksums()
    }

    fn get_months_checksum(&self, y: u32) -> Result<Vec<(u32, Vec<u8>)>> {
        self.storage.get_months_checksum(y)
    }

    fn get_days_checksum(&self, ym: u32) -> Result<Vec<(u32, Vec<u8>)>> {
        self.storage.get_days_checksum(ym)
    }

    fn get_existing_days_in_range(
        &self,
        ymd_from: YearMonthDay,
        ymd_to: YearMonthDay,
    ) -> Result<Vec<YearMonthDay>> {
        self.storage.get_existing_days_in_range(ymd_from, ymd_to)
    }

    fn get_data(&self, ymd: u32) -> Result<Option<Vec<(Data, Vec<Peer>)>>> {
        self.storage.get_photos(ymd)
    }

    fn propose(&self, ymd: u32, data: &[(Vec<u8>, Vec<Peer>)]) -> Result<Vec<u8>> {
        self.storage.add_photos_to_day(ymd, data)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_calc_diff() {
        let loc = vec![(1, vec![0])];
        let rem = vec![(1, vec![0])];
        let res = calc_diff(&loc, &rem);
        assert_eq!(res, (vec![], vec![], vec![]));

        let loc = vec![(1, vec![0]), (2, vec![0])];
        let rem = vec![(1, vec![0])];
        let res = calc_diff(&loc, &rem);
        assert_eq!(res, (vec![], vec![2], vec![]));

        let loc = vec![(2, vec![0])];
        let rem = vec![(1, vec![0]), (2, vec![0]), (3, vec![0])];
        let res = calc_diff(&loc, &rem);
        assert_eq!(res, (vec![1, 3], vec![], vec![]));

        let loc = vec![(1, vec![0])];
        let rem = vec![(1, vec![1])];
        let res = calc_diff(&loc, &rem);
        assert_eq!(res, (vec![], vec![], vec![1]));
    }
}
