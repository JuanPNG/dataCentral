SELECT DISTINCT
    m.stable_id AS gene_stable_id,
    m.description,
    h.families,
    h.gene_trees,
    h.gene_gain_loss_trees,
    h.orthologues,
    h.paralogues,
    h.homoeologues
FROM genome_db as g
JOIN gene_member as m USING(genome_db_id, taxon_id)
JOIN gene_member_hom_stats as h USING(gene_member_id)
-- WHERE taxon_id IN ({string_taxon_ids})
-- LIMIT 10;