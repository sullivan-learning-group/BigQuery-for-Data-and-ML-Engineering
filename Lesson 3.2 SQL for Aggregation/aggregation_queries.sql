-- Query 1: Basic count of products by category
-- Shows simple GROUP BY aggregation
SELECT 
  category,
  COUNT(*) as product_count
FROM `retail_dataset.products`
GROUP BY category
ORDER BY product_count DESC;

-- Query 2: Total sales amount by store
-- Demonstrates simple aggregation with JOIN
SELECT 
  s.store_id,
  s.store_name,
  s.city,
  s.state,
  SUM(t.total_amount) as total_sales
FROM `retail_dataset.sales_transactions` t
JOIN `retail_dataset.stores` s ON t.store_id = s.store_id
GROUP BY s.store_id, s.store_name, s.city, s.state
ORDER BY total_sales DESC;

-- Query 3: Average price by product category
-- Shows numeric aggregation with filtering
SELECT 
  category,
  COUNT(*) as product_count,
  ROUND(AVG(price), 2) as average_price,
  MIN(price) as min_price,
  MAX(price) as max_price
FROM `retail_dataset.products`
WHERE stock_quantity > 0
GROUP BY category
ORDER BY average_price DESC;

-- Query 4: Total sales quantity by product
-- Demonstrates aggregation with multiple joins
SELECT 
  p.product_id,
  p.product_name,
  p.category,
  SUM(t.quantity) as total_quantity_sold,
  SUM(t.total_amount) as total_revenue,
  ROUND(SUM(t.total_amount) / SUM(t.quantity), 2) as average_unit_price
FROM `retail_dataset.sales_transactions` t
JOIN `retail_dataset.products` p ON t.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- Query 5: Sales by location with store count
-- Demonstrates hierarchical aggregation
SELECT 
  s.state,
  s.city,
  COUNT(DISTINCT s.store_id) as store_count,
  SUM(t.total_amount) as total_sales,
  ROUND(SUM(t.total_amount) / COUNT(DISTINCT s.store_id), 2) as avg_sales_per_store
FROM `retail_dataset.sales_transactions` t
JOIN `retail_dataset.stores` s ON t.store_id = s.store_id
GROUP BY s.state, s.city
ORDER BY s.state, total_sales DESC;

-- Query 6: Monthly sales trend analysis
-- Shows date-based aggregation using EXTRACT functions
SELECT 
  EXTRACT(YEAR FROM transaction_date) as year,
  EXTRACT(MONTH FROM transaction_date) as month,
  COUNT(DISTINCT transaction_id) as transaction_count,
  SUM(quantity) as total_items_sold,
  SUM(total_amount) as total_sales
FROM `retail_dataset.sales_transactions`
GROUP BY year, month
ORDER BY year, month;

-- Query 7: Product category performance by state
-- Demonstrates multi-dimensional analysis
SELECT 
  s.state,
  p.category,
  COUNT(DISTINCT t.transaction_id) as transaction_count,
  SUM(t.quantity) as items_sold,
  ROUND(SUM(t.total_amount), 2) as total_sales,
  ROUND(SUM(t.total_amount) / SUM(t.quantity), 2) as avg_price_per_item
FROM `retail_dataset.sales_transactions` t
JOIN `retail_dataset.products` p ON t.product_id = p.product_id
JOIN `retail_dataset.stores` s ON t.store_id = s.store_id
GROUP BY s.state, p.category
ORDER BY s.state, total_sales DESC;

-- Query 8: Day of week sales analysis
-- Uses date functions for business intelligence
SELECT 
  FORMAT_DATE('%A', transaction_date) as day_of_week,
  COUNT(DISTINCT transaction_id) as transaction_count,
  SUM(quantity) as total_items_sold,
  ROUND(SUM(total_amount), 2) as total_sales,
  ROUND(SUM(total_amount) / COUNT(DISTINCT transaction_id), 2) as avg_transaction_value
FROM `retail_dataset.sales_transactions`
GROUP BY day_of_week
ORDER BY CASE
  WHEN day_of_week = 'Monday' THEN 1
  WHEN day_of_week = 'Tuesday' THEN 2
  WHEN day_of_week = 'Wednesday' THEN 3
  WHEN day_of_week = 'Thursday' THEN 4
  WHEN day_of_week = 'Friday' THEN 5
  WHEN day_of_week = 'Saturday' THEN 6
  WHEN day_of_week = 'Sunday' THEN 7
END;

-- Query 9: Sales performance with inventory analysis
-- Demonstrates WITH clause and complex calculations
WITH product_sales AS (
  SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.stock_quantity,
    SUM(t.quantity) as quantity_sold,
    ROUND(SUM(t.total_amount), 2) as total_revenue
  FROM `retail_dataset.products` p
  LEFT JOIN `retail_dataset.sales_transactions` t ON p.product_id = t.product_id
  GROUP BY p.product_id, p.product_name, p.category, p.stock_quantity
)
SELECT 
  category,
  COUNT(*) as product_count,
  SUM(stock_quantity) as total_inventory,
  SUM(quantity_sold) as total_sold,
  ROUND(SUM(total_revenue), 2) as total_revenue,
  ROUND(SUM(quantity_sold) / NULLIF(SUM(stock_quantity), 0), 4) * 100 as inventory_turnover_percent,
  ROUND(AVG(CASE WHEN quantity_sold > 0 THEN total_revenue / quantity_sold ELSE NULL END), 2) as avg_price
FROM product_sales
GROUP BY category
ORDER BY total_revenue DESC;

-- Query 10: Advanced multi-dimensional analysis with window functions
-- Demonstrates window functions for comparative analytics
WITH store_category_sales AS (
  SELECT
    s.store_id,
    s.store_name,
    s.state,
    s.city,
    p.category,
    SUM(t.quantity) as items_sold,
    SUM(t.total_amount) as category_sales,
    
    -- Calculate store ranking within state for each category
    ROW_NUMBER() OVER(PARTITION BY s.state, p.category ORDER BY SUM(t.total_amount) DESC) as state_category_rank,
    
    -- Calculate percent of store's sales from each category
    SUM(t.total_amount) / SUM(SUM(t.total_amount)) OVER(PARTITION BY s.store_id) * 100 as percent_of_store_sales,
    
    -- Calculate percent of total state sales for this category
    SUM(t.total_amount) / SUM(SUM(t.total_amount)) OVER(PARTITION BY s.state, p.category) * 100 as percent_of_state_category_sales
  FROM `retail_dataset.sales_transactions` t
  JOIN `retail_dataset.products` p ON t.product_id = p.product_id
  JOIN `retail_dataset.stores` s ON t.store_id = s.store_id
  GROUP BY s.store_id, s.store_name, s.state, s.city, p.category
)
SELECT
  store_id,
  store_name,
  state,
  city,
  category,
  items_sold,
  ROUND(category_sales, 2) as category_sales,
  state_category_rank,
  ROUND(percent_of_store_sales, 2) as percent_of_store_sales,
  ROUND(percent_of_state_category_sales, 2) as percent_of_state_category_sales,
  CASE 
    WHEN state_category_rank = 1 THEN 'State Leader'
    WHEN state_category_rank <= 3 THEN 'Top Performer'
    ELSE 'Average'
  END as performance_tier
FROM store_category_sales
ORDER BY state, category, state_category_rank;