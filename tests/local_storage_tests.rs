mod common;
use photo_sync_tst::local_storage::LocalStorage;

#[test]
fn test_add_photo_idempotency() -> anyhow::Result<()> {
    let sut: LocalStorage = LocalStorage::test_new()?;

    sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(0))])?;
    let years_checksum_1 = sut.get_years_checksums()?;
    let months_checksum_1 = sut.get_months_checksum(2022)?;
    let days_checksum_1 = sut.get_days_checksum(202201)?;
    let photos_1 = sut.get_photos(20220101)?;

    sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(0))])?;
    let years_checksum_2 = sut.get_years_checksums()?;
    let months_checksum_2 = sut.get_months_checksum(2022)?;
    let days_checksum_2 = sut.get_days_checksum(202201)?;
    let photos_2 = sut.get_photos(20220101)?;

    assert_eq!(years_checksum_1, years_checksum_2);
    assert_eq!(months_checksum_1, months_checksum_2);
    assert_eq!(days_checksum_1, days_checksum_2);
    assert_eq!(photos_1, photos_2);

    Ok(())
}

#[test]
fn test_add_photo_merge_peers() -> anyhow::Result<()> {
    let sut: LocalStorage = LocalStorage::test_new()?;

    sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(0))])?;
    let day_photos = sut.get_photos(20220101)?.unwrap();
    assert_eq!(peers!(0), day_photos[0].1);

    // Adding same photo but with another peer
    sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(1))])?;
    let day_photos = sut.get_photos(20220101)?.unwrap();
    assert_eq!(peers!(0, 1), day_photos[0].1);

    Ok(())
}

#[test]
fn test_add_photo_same_day() -> anyhow::Result<()> {
    let sut: LocalStorage = LocalStorage::test_new()?;

    sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(0))])?;
    let years_checksum_1 = sut.get_years_checksums()?;
    let months_checksum_1 = sut.get_months_checksum(2022)?;
    let days_checksum_1 = sut.get_days_checksum(202201)?;
    let photos_1 = sut.get_photos(20220101)?;
    assert_eq!(1, photos_1.unwrap().len());

    sut.add_photos_to_day(20220101, &vec![(img!(1), peers!(0))])?;
    let years_checksum_2 = sut.get_years_checksums()?;
    let months_checksum_2 = sut.get_months_checksum(2022)?;
    let days_checksum_2 = sut.get_days_checksum(202201)?;
    let photos_2 = sut.get_photos(20220101)?;
    assert_eq!(2, photos_2.unwrap().len());

    assert_ne!(years_checksum_1, years_checksum_2);
    assert_ne!(months_checksum_1, months_checksum_2);
    assert_ne!(days_checksum_1, days_checksum_2);

    Ok(())
}

#[test]
fn test_add_photo_another_month_day() -> anyhow::Result<()> {
    let sut: LocalStorage = LocalStorage::test_new()?;

    sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(0))])?;
    let years_checksum_1 = sut.get_years_checksums()?;
    let months_checksum_1 = sut.get_months_checksum(2022)?;
    let days_1_checksum_1 = sut.get_days_checksum(202201)?;
    let photos_20220101_1 = sut.get_photos(20220101)?;

    sut.add_photos_to_day(20220201, &vec![(img!(1), peers!(0))])?;
    let years_checksum_2 = sut.get_years_checksums()?;
    let months_checksum_2 = sut.get_months_checksum(2022)?;
    let days_1_checksum_2 = sut.get_days_checksum(202201)?;
    let days_2_checksum_2 = sut.get_days_checksum(202202)?;
    let photos_20220101_2 = sut.get_photos(20220101)?;
    let photos_20220201_2 = sut.get_photos(20220201)?;

    assert_ne!(years_checksum_1, years_checksum_2); // checksum of all years changed
    assert_ne!(months_checksum_1, months_checksum_2); // checksum of months for the year changed
    assert_eq!(days_1_checksum_1, days_1_checksum_2); // checksum of days of 1st month unchanged
    assert_ne!(days_1_checksum_1, days_2_checksum_2); // checksum of days for 1st and 2d months different
    assert_eq!(photos_20220101_1, photos_20220101_2);
    assert_ne!(photos_20220101_1, photos_20220201_2);

    Ok(())
}

#[test]
fn test_checksums_do_not_depend_on_order() -> anyhow::Result<()> {
    let (years_1, months_1, days_1) = {
        let sut: LocalStorage = LocalStorage::test_new()?;
        // Adding photos to same day
        sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(0))])?;
        sut.add_photos_to_day(20220101, &vec![(img!(1), peers!(0))])?;
        // To another dau in same month
        sut.add_photos_to_day(20220102, &vec![(img!(0), peers!(0))])?;
        // To another month
        sut.add_photos_to_day(20220201, &vec![(img!(0), peers!(0))])?;

        (
            sut.get_years_checksums()?,
            sut.get_months_checksum(2022)?,
            sut.get_days_checksum(202201)?,
        )
    };

    let (years_2, months_2, days_2) = {
        let sut: LocalStorage = LocalStorage::test_new()?;
        // Doing same, but in another order
        sut.add_photos_to_day(20220201, &vec![(img!(0), peers!(0))])?;
        sut.add_photos_to_day(20220102, &vec![(img!(0), peers!(0))])?;
        sut.add_photos_to_day(20220101, &vec![(img!(1), peers!(0))])?;
        sut.add_photos_to_day(20220101, &vec![(img!(0), peers!(0))])?;

        (
            sut.get_years_checksums()?,
            sut.get_months_checksum(2022)?,
            sut.get_days_checksum(202201)?,
        )
    };

    assert_eq!(years_1, years_2);
    assert_eq!(months_1, months_2);
    assert_eq!(days_1, days_2);

    Ok(())
}
