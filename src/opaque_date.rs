//! To optimize the storage size and performance,
//! we use u32 to encode year, month and day.
//! Year is encoded as is, e.g. year 2025 is just 2015 number.
//!
//! Month is encoded as year and month in the same number,
//! e.g. 2015 Feb will be 201502
//!
//! Day is encoded in the same way as month, but with a day component,
//! e.g. 2015 Feb 17 will be 20150217
//!
//! We don't use new type approach, like `struct YearMonth(u32)`
//! because even when new types are "zero-cost abstraction",
//! the conversion between collections,
//! e.g. Vec<YearMonth> -> Vec<u32> takes time to create an iterator...

use std::ops::RangeInclusive;

pub type Year = u32;

/// Year/month encoded into u32 as yyyymm
pub type YearMonth = u32;

/// Year/month/day encoded into u32 as yyyymmdd
pub type YearMonthDay = u32;

/// For given year, return an interval boundaries for all
/// the days, that belong to this year.
/// E.g. for 2015 it will return [20150101, 20151231]
pub fn ymd_interval_for_y(year: Year) -> (u32, u32) {
    (year * 10000 + 101, year * 10000 + 1231)
}

/// For given year/month, return an interval boundaries for all
/// the days, that belong to this month.
/// E.g. for 201507 it will return [20150701, 20150731]
pub fn ymd_interval_for_ym(year_month: YearMonth) -> (u32, u32) {
    (year_month * 100 + 1, year_month * 100 + 31)
}

pub fn ym_range_for_y(year: Year) -> RangeInclusive<u32> {
    year * 100 + 1..=year * 100 + 12
}

pub fn ymd_range_for_ym(year_month: YearMonth) -> RangeInclusive<u32> {
    year_month * 100 + 1..=year_month * 100 + 31
}

/// Converts u32 encoded year/month/day to year/month
pub fn ymd_to_ym(year_month_day: YearMonthDay) -> YearMonth {
    year_month_day / 100
}

/// Converts u32 encoded year/month to year
pub fn ym_to_y(year_month: YearMonth) -> Year {
    year_month / 100
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ymd_range_for_y() {
        let result: (u32, u32) = ymd_interval_for_y(2020);
        assert_eq!(result, (20200101, 20201231));
    }
}
