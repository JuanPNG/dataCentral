WITH reduced_annotations AS (
  SELECT DISTINCT accession, gene_id
  FROM `<project>.<dataset>.annotations`
),
reduced_gene_trees AS (
  SELECT *
  FROM `<project>.<dataset>.compara_gene_trees`
  WHERE gene_stable_id IN (
    SELECT gene_id FROM reduced_annotations
  )
),
reduced_homo_stats AS (
  SELECT * EXCEPT(description)
  FROM `<project>.<dataset>.compara_homology_stats`
  WHERE gene_stable_id IN (
     SELECT gene_id FROM reduced_annotations
  )
),
reduced_spatial AS (
  SELECT DISTINCT accession, BIOME_NAME, ECO_NAME
  FROM `<project>.<dataset>.bd_spatial_annotations`
)
SELECT
  ra.*,
  rgt.gene_tree_id,
  rgt.clusterset_id,
  rgt.description,
  rhs.gene_trees,
  rhs.gene_gain_loss_trees,
  rhs.families,
  rhs.orthologues,
  rhs.paralogues,
  rhs.homoeologues,
  rs.ECO_NAME,
  rs.BIOME_NAME,
  tax.* EXCEPT(accession, tax_id, gbif_usageKey, gbif_scientificName)
FROM reduced_annotations AS ra
LEFT JOIN reduced_gene_trees AS rgt ON ra.gene_id = rgt.gene_stable_id
LEFT JOIN reduced_homo_stats AS rhs ON ra.gene_id = rhs.gene_stable_id
LEFT JOIN reduced_spatial AS rs ON ra.accession = rs.accession
LEFT JOIN `gbdp.taxonomy` AS tax ON ra.accession = tax.accession
WHERE rgt.gene_tree_id IS NOT NULL;