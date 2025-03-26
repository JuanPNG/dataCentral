SELECT
  accession,
  -- Mean ± standard deviation
  ROUND(AVG(bio1), 2) AS mean_temp,
  ROUND(STDDEV(bio1), 2) AS sd_temp,
  ROUND(AVG(bio5), 2) AS mean_max_temp,
  ROUND(STDDEV(bio5), 2) AS sd_max_temp,
  ROUND(AVG(bio6), 2) AS mean_min_temp,
  ROUND(STDDEV(bio6), 2) AS sd_min_temp,
  ROUND(AVG(bio12), 2) AS mean_precip,
  ROUND(STDDEV(bio12), 2) AS sd_precip,

  -- Median (50th percentile)
  ROUND(APPROX_QUANTILES(bio1, 2)[OFFSET(1)], 2) AS median_temp,
  ROUND(APPROX_QUANTILES(bio5, 2)[OFFSET(1)], 2) AS median_max_temp,
  ROUND(APPROX_QUANTILES(bio6, 2)[OFFSET(1)], 2) AS median_min_temp,
  ROUND(APPROX_QUANTILES(bio12, 2)[OFFSET(1)], 2) AS median_precip,

  -- 5th and 95th percentiles
  ROUND(APPROX_QUANTILES(bio1, 100)[OFFSET(5)], 2) AS temp_5th_percentile,
  ROUND(APPROX_QUANTILES(bio1, 100)[OFFSET(95)], 2) AS temp_95th_percentile,
  ROUND(APPROX_QUANTILES(bio5, 100)[OFFSET(5)], 2) AS max_temp_5th_percentile,
  ROUND(APPROX_QUANTILES(bio5, 100)[OFFSET(95)], 2) AS max_temp_95th_percentile,
  ROUND(APPROX_QUANTILES(bio6, 100)[OFFSET(5)], 2) AS min_temp_5th_percentile,
  ROUND(APPROX_QUANTILES(bio6, 100)[OFFSET(95)], 2) AS min_temp_95th_percentile,
  ROUND(APPROX_QUANTILES(bio12, 100)[OFFSET(5)], 2) AS precip_5th_percentile,
  ROUND(APPROX_QUANTILES(bio12, 100)[OFFSET(95)], 2) AS precip_95th_percentile,

  -- Absolute min/max
  ROUND(MIN(bio1), 2) AS abs_min_temp,
  ROUND(MAX(bio1), 2) AS abs_max_temp,
  ROUND(MIN(bio5), 2) AS abs_min_max_temp,
  ROUND(MAX(bio5), 2) AS abs_max_max_temp,
  ROUND(MIN(bio6), 2) AS abs_min_min_temp,
  ROUND(MAX(bio6), 2) AS abs_max_min_temp,
  ROUND(MIN(bio12), 2) AS abs_min_precip,
  ROUND(MAX(bio12), 2) AS abs_max_precip
FROM `<dataset>.bd_spatial_annotations`
GROUP BY accession