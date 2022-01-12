SELECT
  hc.locationName AS name,
  hc.timeZone AS timeZone,
  l.title AS locationName,
  ARRAY_TO_STRING(l.storefrontAddress.addressLines, ", ") AS addressLines,
  l.storefrontAddress.locality AS locality,
  l.storefrontAddress.administrativeArea AS administrativeArea,
  l.storefrontAddress.postalCode AS postalCode,
  l.storefrontAddress.regionCode AS regionCode,
  metricValues.metric AS metric,
  metricValues.dimensionalValues AS dimensionalValues
FROM
  `<PROJECT_ID>.alligator.hourly_calls` AS hc
  CROSS JOIN
    UNNEST(hc.metricValues) AS metricValues
  JOIN `<PROJECT_ID>.alligator.locations` AS l
    ON REGEXP_REPLACE(hc.locationName, r'^accounts/[^/]+/', '') = l.name
