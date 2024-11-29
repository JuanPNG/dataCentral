WITH
  unique_accessions AS (
  SELECT
    DISTINCT accession
  FROM
    `<project>.<dataset>.annotations` ),
  spatial_annotations AS (
  SELECT
    *
  FROM
    `<project>.<dataset>.bd_spatial_annotations`
  LEFT JOIN (
    SELECT
      species,
      area_km2
    FROM
      `<project>.<dataset>.test_convex_hull` )
  USING
    (species) ),
  transformed_taxonomy AS (
  SELECT
    accession,
    ARRAY_AGG(STRUCT( tax.kingdom,
        tax.phylum,
        tax.class,
        tax.order,
        tax.family,
        tax.genus,
        tax.species )) AS taxonomy
  FROM
    `<project>.<dataset>.test_taxonomy` AS tax
  GROUP BY
    accession ),
  transformed_gbif AS (
  SELECT
    accession,
    ARRAY_AGG(STRUCT( gb.gbif_usageKey,
        gb.decimalLongitude,
        gb.decimalLatitude,
        gb.geodeticDatum,
        gb.coordinateUncertaintyInMeters,
        gb.eventDate,
        gb.countryCode,
        gb.iucnRedListCategory,
        gb.gadm )) AS gbif_occs
  FROM
    `<project>.<dataset>.gbif_occurrences` AS gb
  GROUP BY
    accession ),
  transformed_spatial AS (
  SELECT
    accession,
    ARRAY_AGG(STRUCT( spa.species,
        spa.decimalLongitude,
        spa.decimalLatitude,
        spa.bio1,
        spa.bio2,
        spa.bio3,
        spa.bio4,
        spa.bio5,
        spa.bio6,
        spa.bio7,
        spa.bio8,
        spa.bio9,
        spa.bio10,
        spa.bio11,
        spa.bio12,
        spa.bio13,
        spa.bio14,
        spa.bio15,
        spa.bio16,
        spa.bio17,
        spa.bio18,
        spa.bio19,
        spa.ECO_NAME,
        spa.BIOME_NAME,
        spa.REALM,
        spa.NNH_NAME,
        spa.area_km2 )) AS spatial
  FROM
    spatial_annotations AS spa
  GROUP BY
    accession )
SELECT
  u.accession,
  t.taxonomy,
  g.gbif_occs,
  s.spatial
FROM
  unique_accessions AS u
LEFT JOIN
  transformed_taxonomy AS t
ON
  u.accession = t.accession
LEFT JOIN
  transformed_gbif AS g
ON
  u.accession = g.accession
LEFT JOIN
  transformed_spatial AS s
ON
  u.accession = s.accession