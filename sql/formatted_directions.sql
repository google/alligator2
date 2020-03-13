SELECT
  d.locationName AS name,
  d.timeZone AS timeZone,
  l.locationName AS locationName,
  topDirectionSources.dayCount AS dayCount,
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
