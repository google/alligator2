SELECT
  hc.locationName AS name,
  hc.timeZone AS timeZone,
  l.locationName AS locationName,
  metricValues.metric AS metric,
  metricValues.dimensionalValues AS dimensionalValues
FROM
  `<PROJECT_ID>.alligator.hourly_calls` AS hc
  CROSS JOIN
    UNNEST(hc.metricValues) AS metricValues
  JOIN `<PROJECT_ID>.alligator.locations` AS l
    ON hc.locationName = l.name
