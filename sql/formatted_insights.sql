SELECT
  l.name AS name,
  i.timeZone AS timeZone,
  l.title AS locationName,
  ARRAY_TO_STRING(l.storefrontAddress.addressLines, ", ") AS addressLines,
  l.storefrontAddress.locality AS locality,
  l.storefrontAddress.administrativeArea AS administrativeArea,
  l.storefrontAddress.postalCode AS postalCode,
  l.storefrontAddress.regionCode AS regionCode,
  metricValues.metric AS metric,
  dimensionalValues.value AS value,
  dimensionalValues.metricOption AS metricOption,
  dimensionalValues.timeDimension.timeRange.startTime AS startTime
FROM
  `<PROJECT_ID>.alligator.insights` AS i
  CROSS JOIN
    UNNEST(metricValues) AS metricValues
    ON metricValues.metric IS NOT NULL
  CROSS JOIN
    UNNEST(metricValues.dimensionalValues) AS dimensionalValues
  JOIN `<PROJECT_ID>.alligator.locations` AS l
    ON REGEXP_REPLACE(i.locationName, r'^accounts/[^/]+/', '') = l.name
