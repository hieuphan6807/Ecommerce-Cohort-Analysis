/*  Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month) */

SELECT 
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', CAST(date AS STRING))) AS month 
  ,SUM( totals.visits) AS visits
  ,SUM(totals.pageviews) as pageviews
  ,SUM(totals.transactions) as transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY 1
ORDER BY 1;

/*  Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)  */

WITH raw_data  AS (
  SELECT 
    trafficSource.source as source
    ,SUM( totals.visits) AS total_visits
    ,COUNT(totals.bounces) as total_no_of_bounces
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  GROUP BY trafficSource.source
  ORDER BY total_visits DESC
)
SELECT
  source
  ,total_visits
  ,total_no_of_bounces
  ,ROUND(total_no_of_bounces / total_visits * 100 , 3) AS bounce_rate
FROM raw_data;

/*  Query 3: Revenue by traffic source by week, by month in June 2017 */

WITH month_data AS ( -- tính revenue theo month
  SELECT
    'Month' AS time_type
    ,format_date("%Y%m", parse_date("%Y%m%d", date)) AS time
    ,trafficSource.source AS source
    ,ROUND(SUM(productRevenue / 1000000),4) AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE product.productRevenue IS NOT NULL
  GROUP BY 1,2,3
  ORDER BY revenue DESC
),
week_data AS ( -- tính revenue theo week
  SELECT
    'Week' AS time_type
    ,format_date("%Y%W", parse_date("%Y%m%d", date)) AS time
    ,trafficSource.source AS source
    ,ROUND(SUM(productRevenue / 1000000),4) AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE product.productRevenue IS NOT NULL
  GROUP BY 1,2,3
  ORDER BY revenue DESC
)
SELECT 
  *
FROM month_data

UNION ALL

SELECT 
  *
FROM week_data
ORDER BY time_type;

/*  Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.  */

WITH purchasers AS ( -- calculate purchasers
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
    ,SUM(totals.pageviews) AS total_pageviews_purchase
    ,COUNT(DISTINCT fullVisitorId) AS unique_users_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE _TABLE_SUFFIX BETWEEN '0601' AND '0731' -- get June and July
    AND totals.transactions >= 1 
    AND productRevenue IS NOT NULL
  GROUP BY month
),

non_purchasers AS ( --calculate non_purchasers
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
    ,SUM(totals.pageviews) AS total_pageviews_non_purchase
    ,COUNT(DISTINCT fullVisitorId) AS unique_users_non_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE _TABLE_SUFFIX BETWEEN '0601' AND '0731'
    AND totals.transactions IS NULL 
    AND productRevenue IS NULL
  GROUP BY month
)

SELECT
  COALESCE(p.month, np.month) AS month
  ,ROUND(p.total_pageviews_purchase / p.unique_users_purchase, 7) AS avg_pageviews_purchase
  ,ROUND(np.total_pageviews_non_purchase / np.unique_users_non_purchase,7) AS avg_pageviews_non_purchase
FROM purchasers p
FULL JOIN non_purchasers np 
  ON p.month = np.month
ORDER BY month;

/*  Query 05: Average number of transactions per user that made a purchase in July 2017  */

SELECT
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
  ,SUM(totals.transactions) / COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
WHERE totals.transactions >= 1 
  AND productRevenue IS NOT NULL
GROUP BY month;

/*  Query 06: Average amount of money spent per session. Only include purchaser data in July 2017  */

WITH revenue_per_user AS ( -- money spent per session in July 2017
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
    ,SUM(productRevenue) / 1000000 AS total_revenue
    ,SUM(totals.visits) AS totals_visits
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE totals.transactions IS NOT NULL 
    AND productRevenue IS NOT NULL
  GROUP BY month
)
SELECT
  month 
  ,ROUND(total_revenue / totals_visits, 2) AS avg_revenue_by_user_per_visit
FROM revenue_per_user;

/*  Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017.
Output should show product name and the quantity was ordered. */

WITH raw_data AS ( 
  SELECT
    fullVisitorId
    ,v2ProductName AS product_name
    ,productQuantity AS quantity
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE totals.transactions >= 1
    AND product.productRevenue IS NOT NULL
),

purchased_orders AS ( -- list customers who purchased product "YouTube Men's Vintage Henley"
  SELECT 
    DISTINCT fullVisitorId
  FROM raw_data
  WHERE product_name = "YouTube Men's Vintage Henley"
),

other_purchases AS (  -- other products
  SELECT
    r.product_name AS other_purchased_products
    ,SUM(r.quantity) AS quantity
  FROM raw_data r
  JOIN purchased_orders p
   ON r.fullVisitorId = p.fullVisitorId
  WHERE r.product_name != "YouTube Men's Vintage Henley"
  GROUP BY other_purchased_products
)

SELECT *
FROM other_purchases
ORDER BY quantity DESC;

/*  Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017.
For example: 100% product view then 40% add_to_cart and 10% purchase. */

WITH cte_view AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
    ,COUNT(product.v2ProductName) AS num_product_view
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '2'
    AND product.v2ProductName IS NOT NULL
  GROUP BY month
),

cte_addtocart AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
    ,COUNT(product.v2ProductName) AS num_add_to_card
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '3'
    AND product.v2ProductName IS NOT NULL
  GROUP BY month
),

cte_purchase AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
    ,COUNT(product.v2ProductName) AS num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '6'
    AND product.productRevenue IS NOT NULL
  GROUP BY month
)

SELECT
  COALESCE(v.month, a.month, p.month) AS month
  ,SUM(v.num_product_view) AS num_product_view
  ,SUM(a.num_add_to_card) AS num_addtocart
  ,SUM(p.num_purchase) AS num_purchase
  ,ROUND(SAFE_DIVIDE(SUM(a.num_add_to_card), SUM(v.num_product_view)) * 100, 2) AS add_to_cart_rate
  ,ROUND(SAFE_DIVIDE(SUM(p.num_purchase), SUM(v.num_product_view)) * 100, 2) AS purchase_rate
FROM cte_view v
LEFT JOIN cte_addtocart a
  ON v.month = a.month 
LEFT JOIN cte_purchase p
  ON v.month = p.month 
GROUP BY month
ORDER BY month;

