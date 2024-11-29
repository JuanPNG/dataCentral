WITH
  accessions AS (
  SELECT
    DISTINCT accession
  FROM
    `<project>.<dataset>.annotations` ),
  reduced_gene_biotypes AS (
  SELECT
    DISTINCT accession,
    gene_id,
    gene_biotype
  FROM
    `<project>.<dataset>.annotations` ),
  count_gene_biotypes AS (
  SELECT
    accession,
    gene_biotype,
    COUNT(gene_biotype) AS gene_biotype_count
  FROM
    reduced_gene_biotypes
  GROUP BY
    1,
    2 ),
  gene_biotype_struct AS (
  SELECT
    accession,
    ARRAY_AGG(STRUCT( gene_biotype,
        gene_biotype_count )) AS gene_biotypes
  FROM
    count_gene_biotypes
  GROUP BY
    1 ),
  reduced_transcript_biotypes AS (
  SELECT
    DISTINCT accession,
    transcript_id,
    transcript_biotype
  FROM
    `<project>.<dataset>.annotations` ),
  count_transcript_biotypes AS (
  SELECT
    accession,
    transcript_biotype,
    COUNT(transcript_biotype) AS transcript_biotype_count
  FROM
    reduced_transcript_biotypes
  GROUP BY
    1,
    2 ),
  transcript_biotype_struct AS (
  SELECT
    accession,
    ARRAY_AGG(STRUCT( transcript_biotype,
        transcript_biotype_count )) AS transcript_biotypes
  FROM
    count_transcript_biotypes
  GROUP BY
    1 )
SELECT
  a.accession,
  gbs.gene_biotypes,
  tbs.transcript_biotypes
FROM
  accessions AS a
LEFT JOIN
  gene_biotype_struct AS gbs
ON
  a.accession = gbs.accession
LEFT JOIN
  transcript_biotype_struct AS tbs
ON
  a.accession = tbs.accession