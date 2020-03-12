WITH
  origin AS (
    SELECT 
      locationName AS name, 
      metric.metric AS metric,
      CAST(values.value AS INT64) AS value,
      values.timeDimension.timeRange.startTime AS date
    FROM `your-cloud-project.alligator.insights`, # rename to your cloud project
    UNNEST(metricValues) AS metric,
    UNNEST(metric.dimensionalValues) AS values
  ),
  grouped AS (
    SELECT 
      name,
      date,
      SUM(IF(metric='QUERIES_DIRECT', value, 0)) AS QUERIES_DIRECT,
      SUM(IF(metric='QUERIES_INDIRECT', value, 0)) AS QUERIES_INDIRECT,
      SUM(IF(metric='QUERIES_CHAIN', value, 0)) AS QUERIES_CHAIN,
      SUM(IF(metric='VIEWS_MAPS', value, 0)) AS VIEWS_MAPS,
      SUM(IF(metric='VIEWS_SEARCH', value, 0)) AS VIEWS_SEARCH,
      SUM(IF(metric='ACTIONS_WEBSITE', value, 0)) AS ACTIONS_WEBSITE,
      SUM(IF(metric='ACTIONS_PHONE', value, 0)) AS ACTIONS_PHONE,
      SUM(IF(metric='ACTIONS_DRIVING_DIRECTIONS', value, 0)) AS ACTIONS_DRIVING_DIRECTIONS,
      SUM(IF(metric='PHOTOS_VIEWS_MERCHANT', value, 0)) AS PHOTOS_VIEWS_MERCHANT,
      SUM(IF(metric='PHOTOS_COUNT_MERCHANT', value, 0)) AS PHOTOS_COUNT_MERCHANT,
      SUM(IF(metric='PHOTOS_VIEWS_CUSTOMERS', value, 0)) AS PHOTOS_VIEWS_CUSTOMERS,
      SUM(IF(metric='PHOTOS_COUNT_CUSTOMERS', value, 0)) AS PHOTOS_COUNT_CUSTOMERS,
      SUM(IF(metric='LOCAL_POST_VIEWS_SEARCH', value, 0)) AS LOCAL_POST_VIEWS_SEARCH,
      SUM(IF(metric='LOCAL_POST_ACTIONS_CALL_TO_ACTION', value, 0)) AS LOCAL_POST_ACTIONS_CALL_TO_ACTION
    FROM origin
    GROUP BY 1, 2
  )
SELECT
  CASE
    WHEN STARTS_WITH(name, "accounts/000000000000000000001") THEN "Location Group 1" # list your account ID for each location group separately
    WHEN STARTS_WITH(name, "accounts/000000000000000000002") THEN "Location Group 2" # remove the CASE statement if you only use a single location group
    ELSE "Unknown"
  END AS banner, # use 'SELECT name AS banner' if you only use a single location group
  REGEXP_REPLACE(name, r"^accounts/([0-9]+)/.*$", "\\1") AS account,
  REGEXP_REPLACE(name, r"^.*locations/([0-9]+)$", "\\1") AS location,
  locationName AS location_name,
  ARRAY_TO_STRING([ARRAY_TO_STRING(address.addressLines, ", "), address.locality, address.administrativeArea, address.postalCode, address.regionCode], ". ") AS location_address,
  address.locality AS city,
  address.postalCode AS postal,
  storeCode AS store_code,
  labels AS labels,
  date,
  SUBSTR(date, 0,10) AS day,
  QUERIES_DIRECT,
  QUERIES_INDIRECT,
  QUERIES_CHAIN,
  VIEWS_MAPS,
  VIEWS_SEARCH,
  ACTIONS_WEBSITE,
  ACTIONS_PHONE,
  ACTIONS_DRIVING_DIRECTIONS,
  PHOTOS_VIEWS_MERCHANT,
  PHOTOS_COUNT_MERCHANT,
  PHOTOS_VIEWS_CUSTOMERS,
  PHOTOS_COUNT_CUSTOMERS,
  LOCAL_POST_VIEWS_SEARCH,
  LOCAL_POST_ACTIONS_CALL_TO_ACTION,
  CASE
    WHEN QUERIES_INDIRECT = 0 THEN 1
    ELSE (QUERIES_DIRECT / QUERIES_INDIRECT)
  END AS Ratio
FROM `your-cloud-project.alligator.locations` # rename to your cloud project
LEFT JOIN grouped USING (name);
