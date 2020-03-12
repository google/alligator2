SELECT
  CASE
    WHEN STARTS_WITH(name, "accounts/000000000000000000001") THEN "Location Group 1" # list your account ID for each location group separately
    WHEN STARTS_WITH(name, "accounts/000000000000000000002") THEN "Location Group 2" # remove the CASE statement if you only use a single location group
    ELSE "Unknown"
  END AS banner, # use 'SELECT name AS banner' if you only use a single location group
  REGEXP_REPLACE(loc.name, r"^accounts/([0-9]+)/.*$", "\\1") AS account,
  REGEXP_REPLACE(loc.name, r"^.*locations/([0-9]+)$", "\\1") AS location,
  locationName AS location_name,
  ARRAY_TO_STRING([ARRAY_TO_STRING(address.addressLines, ", "), address.locality, address.administrativeArea, address.postalCode, address.regionCode], ". ") AS location_address,
  address.locality AS city,
  address.postalCode AS postal,
  storeCode AS store_code,
  labels AS labels,
  rev.name AS reviewer_name,
  starRating AS star_rating,
  CASE starRating
    WHEN "ONE" THEN 1
    WHEN "TWO" THEN 2
    WHEN "THREE" THEN 3
    WHEN "FOUR" THEN 4
    WHEN "FIVE" THEN 5
    ELSE 0
  END as star_rating_number,
  comment AS review_text,
  updateTime AS review_updated,
  createTime AS review_created,
  reviewReply.comment AS review_reply_text,
  reviewReply.updateTime AS review_reply_update,
  FORMAT_TIMESTAMP("%Y-%m-%d", createTime) AS date
FROM `your-cloud-project.alligator.reviews` rev # rename to your cloud project
LEFT JOIN `your-cloud-project.alligator.locations` loc # rename to your cloud project
  ON loc.name = REGEXP_EXTRACT(rev.name, r"^accounts/[0-9]+/locations/[0-9]+");
