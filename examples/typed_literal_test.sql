SELECT
  reservation_id,
  cleared_date,
  DATE_TRUNC(cleared_date, MONTH) AS cleared_month,
  total
FROM `myproj.dataset.cleared_amounts`
WHERE cleared_date >= DATE '2025-05-01'
