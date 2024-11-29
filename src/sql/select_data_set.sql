SELECT
  DISTINCT a.accession,
  -- tax.species,
  -- tax.order,
  spat.area_km2,
  gbt.gene_biotype_count,
  -- gbif.iucnRedListCategory,
  -- country.name,
  -- spat.REALM,
  -- spat.ECO_NAME,
  -- spat.BIOME_NAME,
  -- spat.bio1,
  -- spat.bio2, -- Y
  -- spat.bio3,
  -- spat.bio4,
  -- spat.bio5,
  -- spat.bio6,
  -- spat.bio7,
  -- spat.bio8,
  -- spat.bio9,
  -- spat.bio10,
  -- spat.bio11, -- Y
  -- spat.bio12,
  -- spat.bio13, -- Y
  -- spat.bio14,
  -- spat.bio15, -- Y
  -- spat.bio16,
  -- spat.bio17,
  -- spat.bio18,
  -- spat.bio19
FROM
  `<project>.<dataset>.all_annotations` AS a,
  a.taxonomy AS tax,
  a.gene_biotypes AS gbt,
  a.gbif_occs AS gbif,
  gbif.gadm AS gadm,
  gadm.level0 AS country,
  a.spatial AS spat
WHERE
  gbt.gene_biotype = 'protein_coding'
AND spat.area_km2 > 0
--   AND tax.order = 'Coleoptera'
ORDER BY spat.area_km2