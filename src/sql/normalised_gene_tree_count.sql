WITH biome_gene_count AS (
  SELECT
    gene_tree_id,
    BIOME_NAME,
    COUNT(gene_id) AS gene_count
  FROM `<dataset>.annotations_gene_trees`
  WHERE BIOME_NAME IS NOT NULL
  GROUP BY 1, 2
),
stats AS (
  SELECT
    MIN(gene_count) AS min_gene_count,
    MAX(gene_count) AS max_gene_count,
    AVG(gene_count) AS mean_gene_count,
    STDDEV(gene_count) AS stddev_gene_count
  FROM biome_gene_count
)
SELECT
  bgc.*,
  (bgc.gene_count - s.min_gene_count) / NULLIF(s.max_gene_count - s.min_gene_count, 0) AS min_max_normalised_gene_count,
  (bgc.gene_count - s.mean_gene_count) / NULLIF(s.stddev_gene_count, 0) AS z_normalized_value
FROM biome_gene_count AS bgc
CROSS JOIN stats AS s
