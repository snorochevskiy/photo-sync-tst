use crate::opaque_date::*;
use anyhow::Result;
use itertools::Itertools;
use redb::{backends::InMemoryBackend, TableError};
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction};
use sha2::{Digest, Sha256};
use std::path::Path;

pub type Data = Vec<u8>;
pub type Checksum = Vec<u8>;
pub type Peer = Vec<u8>;

/// Following three tables do store checksums for the partitioned data we store.
/// The data is partitioned by year, month and day, that's why this tree like storage of checksums
/// significantly speeds up the search of differences between peers.
///
///  peer1     <--->  peer2
///    years            years
///   // | \\          // | \\
///    months           months
///  // ||| \\        // ||| \\
///     days             days
///
const TBL_CHECKSUM_YEAR: TableDefinition<Year, Checksum> = TableDefinition::new("checksum_year");
const TBL_CHECKSUM_MONTH: TableDefinition<YearMonth, Checksum> =
    TableDefinition::new("checksum_month");
const TBL_CHECKSUM_DAY: TableDefinition<YearMonthDay, Checksum> =
    TableDefinition::new("checksum_day");

const TBL_DATA: TableDefinition<YearMonthDay, Vec<(Data, Vec<Peer>)>> =
    TableDefinition::new("data_in_day");

/// Represents a local object ids (hash) storage which is a local part of a distributed catalog system.
/// The catalog is designed in the way that helps to identify disrepancies with other peers:
/// * object ids are partitioned by year, month and day
/// * each level of partitioning contains checksum (sha256 hash) of it's direct content
/// * for each object id we keep a list of labels - peers that keep the binary data identified by the id
/// When an id is changed for a day, the upgoing chain of checksums is recalculated
pub struct LocalStorage {
    db: Database,
}

impl LocalStorage {
    /// Create a new instance of the local storage
    /// Args:
    /// * path - local DB file
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;
        // redb will automatically detect and recover from crashes,
        // power loss, and other unclean shutdowns.
        Ok(LocalStorage { db })
    }

    /// In memory version of storage, for testing purposes
    pub fn test_new() -> Result<Self> {
        let db = Database::builder().create_with_backend(InMemoryBackend::new())?;
        Ok(LocalStorage { db })
    }

    /// Returns list of all year (the object ids exist for) along with checksums for these years.
    /// The checksum of the is calculated as a checksum of all nested months.
    pub fn get_years_checksums(&self) -> Result<Vec<(Year, Checksum)>> {
        let read_txn = self.db.begin_read()?;
        let table_checksum_year = match read_txn.open_table(TBL_CHECKSUM_YEAR) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(..)) => return Ok(Vec::new()),
            Err(other) => return Err(other.into()),
        };
        let mut result: Vec<(Year, Vec<u8>)> =
            Vec::with_capacity(table_checksum_year.len()? as usize);
        for record in table_checksum_year.iter()? {
            let (year, checksum) = record?;
            result.push((year.value(), checksum.value()))
        }
        Ok(result)
    }

    /// For given year returns a list of nested months with their checksums, that calculated
    /// all as a hash of their nested days
    /// Args:
    /// * y - desired year
    pub fn get_months_checksum(&self, y: Year) -> Result<Vec<(YearMonth, Checksum)>> {
        let read_txn = self.db.begin_read()?;
        let table_checksum_month = match read_txn.open_table(TBL_CHECKSUM_MONTH) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(..)) => return Ok(Vec::new()),
            Err(other) => return Err(other.into()),
        };
        let res_range = table_checksum_month.range(ym_range_for_y(y))?;
        let mut result = Vec::new();
        for ym_checksum_res in res_range {
            let (ym, checksum) = ym_checksum_res?;
            result.push((ym.value(), checksum.value()));
        }
        Ok(result)
    }

    /// For given month returns a list of nested days with their checksums, that calculated
    /// all as a hash of their nested object ids
    /// Args:
    /// * ym - year and month encoded in same integer as ${year number}${month number}.
    ///   e.g. May 2015 is encoded as 201505
    pub fn get_days_checksum(&self, ym: YearMonth) -> Result<Vec<(YearMonthDay, Checksum)>> {
        let read_txn = self.db.begin_read()?;
        let table_checksum_day = match read_txn.open_table(TBL_CHECKSUM_DAY) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(..)) => return Ok(Vec::new()),
            Err(other) => return Err(other.into()),
        };
        let res_range = table_checksum_day.range(ymd_range_for_ym(ym))?;
        let mut result = Vec::new();
        for ym_checksum_res in res_range {
            let (ym, checksum) = ym_checksum_res?;
            result.push((ym.value(), checksum.value()));
        }
        Ok(result)
    }

    /// Return a list of all available (object IDs exist for them) days in a given range defined by start day and end day.
    /// Here, a day is a full date (i.e. year-month-day) encoded into a single 32 unsigned int.
    /// E.g. 2015 May 3 is encoded as 20150503.
    /// Args:
    /// * ymd_from - start day of the interval
    /// * ymd_to - end day of the interval, inclusive
    pub fn get_existing_days_in_range(
        &self,
        ymd_from: YearMonthDay,
        ymd_to: YearMonthDay,
    ) -> Result<Vec<YearMonthDay>> {
        let read_txn = self.db.begin_read()?;
        let table_checksum_day = match read_txn.open_table(TBL_CHECKSUM_DAY) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(..)) => return Ok(Vec::new()),
            Err(other) => return Err(other.into()),
        };
        let range = table_checksum_day.range(ymd_from..=ymd_to)?;
        let mut result = Vec::new();
        for e in range {
            result.push(e?.0.value());
        }
        Ok(result)
    }

    pub fn get_photos(&self, ymd: YearMonthDay) -> Result<Option<Vec<(Data, Vec<Peer>)>>> {
        let read_txn = self.db.begin_read()?;
        let table_days = match read_txn.open_table(TBL_DATA) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(..)) => return Ok(None),
            Err(other) => return Err(other.into()),
        };
        let result = table_days.get(ymd)?.map(|v| v.value());
        Ok(result)
    }

    /// Add list of object ids for given day.
    /// This function can be called when a local data is added and we need to add object IDs pointing to this data,
    /// or during the synchronization with other peers.
    /// Returns resulting hash of the directory
    pub fn add_photos_to_day(
        &self,
        ymd: YearMonthDay,
        new_photos: &[(Data, Vec<Peer>)],
    ) -> Result<Vec<u8>> {
        let write_txn = self.db.begin_write()?; // Only one write transaction can be openned at a time
        let result = {
            let mut table_days = write_txn.open_table(TBL_DATA)?;
            let mut photos = table_days
                .get(ymd)?
                .map(|v| v.value())
                .unwrap_or(Vec::new());

            for new_photo in new_photos {
                // In case if there are a lot of photo, we can optimize this check using bloom folter
                if let Some(element) = photos.iter_mut().find(|(d, _)| *d == new_photo.0) {
                    let peers_to_add = new_photo
                        .1
                        .iter()
                        .filter(|&p| !element.1.contains(p))
                        .map(|e| e.to_owned())
                        .collect_vec();
                    element.1.extend(peers_to_add);
                } else {
                    photos.push(new_photo.clone());
                }
            }

            photos.sort();
            table_days.insert(ymd, &photos)?;

            let new_checksum = calc_photos_checksum(&photos);
            Self::update_day_checksum(&write_txn, ymd, new_checksum.clone())?;
            new_checksum
        };
        write_txn.commit()?;

        Ok(result)
    }

    /// Updates the while upgoing chain of checksums: year/month/day -> year/month -> year
    /// Should be called after the list of object IDs has been chenged for a day.
    /// Args:
    /// * txn - redb transaction
    /// * day - that received an update of object IDs list
    /// * day_checksum - new checksum of the given day
    fn update_day_checksum(
        txn: &WriteTransaction,
        ymd: YearMonthDay,
        day_checksum: Vec<u8>,
    ) -> Result<()> {
        // Updating YearMonthDay checksum table
        let mut table_checksum_day = txn.open_table(TBL_CHECKSUM_DAY)?;
        table_checksum_day.insert(ymd, day_checksum)?;

        // Updating YearMonth checksum table
        let ym = ymd_to_ym(ymd);
        let mut days_checksum_hasher = Sha256::new();
        for day_checksum_res in table_checksum_day.range(ymd_range_for_ym(ym))? {
            // They are allways sorted
            days_checksum_hasher.update(day_checksum_res?.1.value());
        }

        let mut table_checksum_month = txn.open_table(TBL_CHECKSUM_MONTH)?;
        table_checksum_month.insert(ym, days_checksum_hasher.finalize().to_vec())?;

        // Updating Year checksum table
        let y = ym_to_y(ym);
        let mut months_checksum_hasher = Sha256::new();
        for month_checksum_res in table_checksum_month.range(ym_range_for_y(y))? {
            months_checksum_hasher.update(month_checksum_res?.1.value());
        }
        let mut table_checksum_year = txn.open_table(TBL_CHECKSUM_YEAR)?;
        table_checksum_year.insert(y, months_checksum_hasher.finalize().to_vec())?;

        Ok(())
    }

    /// For testing purposes only.
    pub fn dbg_print(&self) -> Result<()> {
        let read_txn = self.db.begin_read()?;
        let table_days = read_txn.open_table(TBL_DATA)?;
        for row_res in table_days.iter()? {
            let row = row_res?;
            println!("{}: {:?}", row.0.value(), row.1.value());
        }
        Ok(())
    }
}

/// Calculates checksum for given list of object IDs
/// that suppose to be taken from a day.
fn calc_photos_checksum(photos: &[(Data, Vec<Peer>)]) -> Checksum {
    let mut hasher = Sha256::new();
    for photo in photos {
        hasher.update(&photo.0);
    }
    hasher.finalize().to_vec()
}
