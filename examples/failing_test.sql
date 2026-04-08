SELECT
  user_id,
  SUM(amount) AS total_amount
FROM `myproj.dataset.orders`
GROUP BY user_id
