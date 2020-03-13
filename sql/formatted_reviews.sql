SELECT
  r.updateTime AS updateTime,
  r.comment AS comment,
  r.reviewer.displayName,
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
  l.locationName AS locationName,
  ARRAY_TO_STRING(l.address.addressLines, ", ") AS addressLines,
  l.address.locality AS locality,
  l.address.administrativeArea AS administrativeArea,
  l.address.postalCode AS postalCode,
  l.address.regionCode AS regionCode
FROM
  `<PROJECT_ID>.alligator.reviews` AS r
  JOIN `<PROJECT_ID>.alligator.locations` AS l
    ON REGEXP_EXTRACT(r.name, '(.*)/reviews/') = l.name
