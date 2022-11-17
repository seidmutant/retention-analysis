WITH transactions AS (
  SELECT from_address, DATE(block_timestamp) AS created_at
  FROM `bigquery-public-data.crypto_ethereum.transactions`
),

cohort AS (
  SELECT 
    from_address,
    MIN(
      CAST(FORMAT_DATETIME('%Y-%m', PARSE_DATETIME('%Y-%m-%d', CAST(DATE(created_at) AS STRING))) || '-01' AS DATE)
    ) AS cohort_month
  FROM transactions
  GROUP BY 1
),

cohort_size AS (
  SELECT
    cohort_month,
    COUNT(1) as num_users
  FROM cohort
  GROUP BY cohort_month
),

user_activities AS (
  SELECT
    DISTINCT
      DATE_DIFF(
        EXTRACT(DATE FROM TIMESTAMP(created_at)),
        cohort_month,
        MONTH
      ) as month_number,
      A.from_address
  FROM transactions AS A
  LEFT JOIN cohort AS C 
  ON A.from_address = C.from_address
),

retention_table AS (
  SELECT
    cohort_month,
    A.month_number,
    COUNT(1) AS num_users
  FROM user_activities A
  LEFT JOIN cohort AS C 
  ON A.from_address = C.from_address
  GROUP BY 1, 2  
)

SELECT
  A.cohort_month,
  B.num_users AS total_users,
  A.month_number,
  ROUND(CAST(A.num_users as float64) * 100 / B.num_users, 2) as percentage
FROM retention_table AS A
LEFT JOIN cohort_size AS B
ON A.cohort_month = B.cohort_month
WHERE 
  A.cohort_month IS NOT NULL
  AND A.cohort_month >= '2022-01-01' AND A.cohort_month < '2022-11-01'
ORDER BY 1, 3
