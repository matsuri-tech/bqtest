CREATE OR REPLACE TABLE `myproj.dataset.user_totals` AS
SELECT
  user_id,
  SUM(amount) AS total_amount
FROM `myproj.dataset.orders`
GROUP BY user_id
