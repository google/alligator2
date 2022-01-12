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
  l.title AS locationName,
  ARRAY_TO_STRING(l.storefrontAddress.addressLines, ", ") AS addressLines,
  l.storefrontAddress.locality AS locality,
  l.storefrontAddress.administrativeArea AS administrativeArea,
  l.storefrontAddress.postalCode AS postalCode,
  l.storefrontAddress.regionCode AS regionCode
FROM
  `<PROJECT_ID>.alligator.reviews` AS r
JOIN `<PROJECT_ID>.alligator.locations` AS l
  ON l.name = REGEXP_EXTRACT(r.name, '^accounts/[^/]+/(.*)/reviews/')
