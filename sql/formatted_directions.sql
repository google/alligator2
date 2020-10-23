SELECT
  d.locationName AS name,
  d.timeZone AS timeZone,
  l.locationName AS locationName,
  topDirectionSources.dayCount AS dayCount,
  ARRAY_TO_STRING(l.address.addressLines, ", ") AS addressLines,
  l.address.locality AS locality,
  l.address.administrativeArea AS administrativeArea,
  l.address.postalCode AS postalCode,
  l.address.regionCode AS regionCode,
  regionCounts.count AS count,
  regionCounts.label AS label,
  regionCounts.latlng.latitude AS latitude,
  regionCounts.latlng.longitude AS longitude
FROM
  `<PROJECT_ID>.alligator.directions` AS d
  CROSS JOIN
    UNNEST(d.topDirectionSources) AS topDirectionSources
  CROSS JOIN
    UNNEST(topDirectionSources.regionCounts) AS regionCounts
  JOIN `<PROJECT_ID>.alligator.locations` AS l
    ON d.locationName = l.name
