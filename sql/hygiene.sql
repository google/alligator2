SELECT
  locationName,
  ARRAY_TO_STRING(address.addressLines, ", ") AS addressLines,
  address.locality,
  address.administrativeArea,
  address.postalCode,
  address.regionCode,
  IF(ARRAY_LENGTH(address.addressLines) IS NULL, FALSE, TRUE) AS hasAddress,
  IF(regularHours IS NULL, FALSE, TRUE) AS hasHours,
  IF(websiteUrl IS NULL, FALSE, TRUE) AS hasWebsite,
  IF(latlng IS NULL, FALSE, TRUE) AS hasLatlong,
  IF(primaryPhone IS NULL, FALSE, TRUE) AS hasPhone,
  locationState.isDisabled IS TRUE AS isDisabled,
  locationState.isPublished IS TRUE AS isPublished,
  locationState.isVerified IS TRUE AS isVerified
FROM
  `<PROJECT_ID>.alligator.locations`
