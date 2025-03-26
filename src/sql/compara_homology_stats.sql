SELECT DISTINCT
    gm.stable_id AS gene_stable_id,
    gm.display_label AS gene_name,
    gm.description,
    h.families,
    h.gene_trees,
    h.gene_gain_loss_trees,
    h.orthologues,
    h.paralogues,
    h.homoeologues
FROM genome_db as g
JOIN gene_member as gm USING(genome_db_id, taxon_id)
JOIN gene_member_hom_stats as h USING(gene_member_id)
-- WHERE taxon_id IN ({string_taxon_ids})
-- LIMIT 10;