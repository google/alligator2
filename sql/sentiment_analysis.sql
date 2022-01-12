SELECT
  r.name AS name,
  r.updateTime AS updateTime,
  r.comment AS comment,
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
  l.title AS locationName,
  ARRAY_TO_STRING(l.storeFrontAddress.addressLines, ", ") AS addressLines,
  l.storeFrontAddress.locality AS locality,
  l.storeFrontAddress.administrativeArea AS administrativeArea,
  l.storeFrontAddress.postalCode AS postalCode,
  l.storeFrontAddress.regionCode AS regionCode,
  s.annotation.documentSentiment.score AS documentSentimentScore,
  sentences.text.content AS sentenceContent,
  sentences.sentiment.score AS sentenceScore,
  entities.name AS entityName,
  entities.salience AS entitySalience,
  s.topic AS topic # remove if not using the topic_clustering feature
FROM
  `<PROJECT_ID>.alligator.sentiments` AS s
  CROSS JOIN
    UNNEST(s.annotation.entities) AS entities
  CROSS JOIN
    UNNEST(s.annotation.sentences) AS sentences
  JOIN `<PROJECT_ID>.alligator.reviews` AS r
    ON s.name = r.name
  JOIN `<PROJECT_ID>.alligator.locations` AS l
    ON REGEXP_EXTRACT(s.name, '^accounts/[^/]+/(.*)/reviews/') = l.name
GROUP BY
  name,
  updateTime,
  comment,
  starRating,
  numRating,
  locationName,
  addressLines,
  locality,
  administrativeArea,
  postalCode,
  regionCode,
  documentSentimentScore,
  sentenceContent,
  sentenceScore,
  entityName,
  entitySalience,
  topic # remove if not using the topic_clustering feature
