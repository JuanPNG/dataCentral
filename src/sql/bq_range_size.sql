WITH geopoints AS (

  SELECT
    species,
    ST_GEOGPOINT(decimalLongitude, decimalLatitude) AS coordinates,
  FROM `<project>.<dataset>.gbif_occurrences`

),
ch AS (

  SELECT
  species,
  ST_CONVEXHULL(ST_UNION_AGG(coordinates)) as convex_hull
  FROM geopoints
  GROUP BY species

)
SELECT
  species,
  ST_AREA(convex_hull) / 1000000 AS area_km2
FROM ch
ORDER BY area_km2 DESC