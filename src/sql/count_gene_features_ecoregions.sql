SELECT
  DISTINCT spa.REALM,
  spa.ECO_NAME,
  spa.BIOME_NAME,
  gbt.gene_biotype,
  COUNT(DISTINCT spa.species) AS n_sp,
  ROUND(AVG(gbt.gene_biotype_count), 2) AS avg_gb,
  MAX(gbt.gene_biotype_count) AS max_gb,
  MIN(gbt.gene_biotype_count) AS min_gb,
  SUM(gbt.gene_biotype_count) AS total_annotations,
FROM
  `gbdp.all_annotations` AS a,
  a.spatial AS spa,
  a.gene_biotypes AS gbt
WHERE
  gene_biotype IS NOT NULL
GROUP BY
  ROLLUP ( spa.REALM,
    spa.ECO_NAME,
    spa.BIOME_NAME,
    gbt.gene_biotype)