CREATE OR REPLACE MODEL
  `gbdp.test_lg_protc_area_size` OPTIONS ( model_type='LINEAR_REG',
    input_label_cols=['gene_biotype_count'],
    optimize_strategy='AUTO_STRATEGY',
    learn_rate_strategy='LINE_SEARCH',
    calculate_p_values=TRUE,
    fit_intercept=TRUE,
    MAX_ITERATIONS=20,
    data_split_method='RANDOM',
    data_split_eval_fraction=0.2,
    num_trials=5,
    l2_reg=HPARAM_RANGE(0,
      1),
    enable_global_explain=TRUE,
    CATEGORY_ENCODING_METHOD='DUMMY_ENCODING' ) AS
WITH
  lg_dataset AS (
  SELECT
    DISTINCT a.accession,
    gbt.gene_biotype_count,
    spat.area_km2
  FROM
    `<project>.<dataset>.all_annotations` AS a,
    a.gene_biotypes AS gbt,
    a.spatial AS spat
  WHERE
    -- Select the gene biotype
    gbt.gene_biotype = 'protein_coding'
    AND spat.area_km2 > 0
    -- GCA_905147385.1 (L. camilla) must be excluded. Associated with data from different species in GBIF.
    AND a.accession != 'GCA_905147385.1' )
SELECT
  gene_biotype_count,
  area_km2
FROM
  lg_dataset