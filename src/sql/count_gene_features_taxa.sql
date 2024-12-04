SELECT
  DISTINCT tax.kingdom,
  tax.phylum,
  tax.class,
  tax.order,
  tax.family,
  tax.genus,
  gbt.gene_biotype,
  COUNT(DISTINCT tax.species) AS n_sp,
  ROUND(AVG(gbt.gene_biotype_count), 2) AS avg_gb,
  MAX(gbt.gene_biotype_count) AS max_gb,
  MIN(gbt.gene_biotype_count) AS min_gb,
  SUM(gbt.gene_biotype_count) AS total_annotations,
FROM
  `<project>.<dataset>.all_annotations` AS a,
  a.taxonomy AS tax,
  a.gene_biotypes AS gbt
WHERE
  gene_biotype IS NOT NULL
GROUP BY
  ROLLUP (tax.kingdom,
    tax.phylum,
    tax.class,
    tax.order,
    tax.family,
    tax.genus,
    gbt.gene_biotype)