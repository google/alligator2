SELECT
  i.locationName AS name,
  i.timeZone AS timeZone,
  l.locationName AS locationName,
  metricValues.metric AS metric,
  dimensionalValues.value AS value,
  dimensionalValues.metricOption AS metricOption,
  dimensionalValues.timeDimension.timeRange.startTime AS startTime
FROM
  `<PROJECT_ID>.alligator.insights` AS i
  CROSS JOIN
    UNNEST(metricValues) AS metricValues
  CROSS JOIN
    UNNEST(metricValues.dimensionalValues) AS dimensionalValues
  JOIN `<PROJECT_ID>.alligator.locations` AS l
    ON i.locationName = l.name
