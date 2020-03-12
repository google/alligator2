SELECT
  CASE
    WHEN STARTS_WITH(l.name, "accounts/000000000000000000001") THEN "Location Group 1" # list your account ID for each location group separately
    WHEN STARTS_WITH(l.name, "accounts/000000000000000000002") THEN "Location Group 2" # remove the CASE statement if you only use a single location group
    ELSE "Unknown"
  END AS banner, # use 'SELECT l.name AS banner' if you only use a single location group
  REGEXP_EXTRACT(l.name, '^accounts/([0-9]+)/.*$') AS account,
  REGEXP_EXTRACT(l.name, '^.*locations/([0-9]+)$') AS location,
  l.locationName AS locationName,
  i.locationName AS name,
  i.timeZone AS timeZone,
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
    ON i.locationName = l.name
