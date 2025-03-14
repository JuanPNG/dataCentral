# Courtesy of Thomas Walsh
SELECT
    gtr.clusterset_id,
    IFNULL(CONCAT(gtr.stable_id, '.', gtr.version), CONCAT('Node_', gtr.root_id)) AS gene_tree_id,
    gm.stable_id AS gene_stable_id,
    gm.description
FROM
    gene_tree_root AS gtr
    JOIN gene_tree_node AS gtn ON gtn.root_id = gtr.root_id
    JOIN gene_member AS gm ON gtn.seq_member_id = gm.canonical_member_id
WHERE
    gtr.tree_type = 'tree'
AND
    gtr.ref_root_id IS NULL;