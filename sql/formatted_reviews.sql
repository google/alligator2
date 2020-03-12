SELECT
  CASE
    WHEN STARTS_WITH(l.name, "accounts/000000000000000000001") THEN "Location Group 1" # list your account ID for each location group separately
    WHEN STARTS_WITH(l.name, "accounts/000000000000000000002") THEN "Location Group 2" # remove the CASE statement if you only use a single location group
    ELSE "Unknown"
  END AS banner, # use 'SELECT l.name AS banner' if you only use a single location group
  REGEXP_EXTRACT(l.name, '^accounts/([0-9]+)/.*$') AS account,
  REGEXP_EXTRACT(l.name, '^.*locations/([0-9]+)$') AS location,
  l.locationName AS locationName,
  l.address.locality AS locality,
  l.address.administrativeArea AS administrativeArea,
  l.address.postalCode AS postalCode,
  l.address.regionCode AS regionCode,
  l.storeCode AS storeCode,
  l.labels AS labels,
  r.createTime as createTime,
  r.updateTime AS updateTime,
  r.comment AS comment,
  r.reviewer.displayName AS reviewerName,
  r.starRating AS starRating,
  (
    CASE
      WHEN r.starRating = "ONE" THEN 1
      WHEN r.starRating = "TWO" THEN 2
      WHEN r.starRating = "THREE" THEN 3
      WHEN r.starRating = "FOUR" THEN 4
      WHEN r.starRating = "FIVE" THEN 5
      ELSE NULL
    END
  ) AS numRating,
  IF(r.reviewReply IS NULL, FALSE, TRUE) AS hasReply,
  reviewReply.comment AS reviewReplyComment,
  reviewReply.updateTime AS reviewReplyUpdate
FROM 
  `<PROJECT_ID>.alligator.reviews` AS r
JOIN `<PROJECT_ID>.alligator.locations` AS l
  ON l.name = REGEXP_EXTRACT(r.name, '(.*)/reviews/')
