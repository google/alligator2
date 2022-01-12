SELECT
  title AS locationName,
  ARRAY_TO_STRING(storefrontAddress.addressLines, ", ") AS addressLines,
  storefrontAddress.locality,
  storefrontAddress.administrativeArea,
  storefrontAddress.postalCode,
  storefrontAddress.regionCode,
  IF(ARRAY_LENGTH(storefrontAddress.addressLines) IS NULL, FALSE, TRUE) AS hasAddress,
  IF(regularHours IS NULL, FALSE, TRUE) AS hasHours,
  IF(websiteUri IS NULL, FALSE, TRUE) AS hasWebsite,
  IF(latlng IS NULL, FALSE, TRUE) AS hasLatlong,
  IF(phoneNumbers.primaryPhone IS NULL, FALSE, TRUE) AS hasPhone,
  locationState.isDisabled IS TRUE AS isDisabled,
  locationState.isPublished IS TRUE AS isPublished,
  locationState.isVerified IS TRUE AS isVerified
FROM
  `<PROJECT_ID>.alligator.locations`
