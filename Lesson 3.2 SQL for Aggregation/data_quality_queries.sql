-- ***********************************************************
-- Data Quality Checks for products Table
-- Project: bq-data-ml-engineering
-- Dataset: retail_dataset
-- Table: products
-- ***********************************************************

-- Check 1: Count NULLs in key columns
SELECT
  COUNTIF(product_id IS NULL) AS null_product_id_count,
  COUNTIF(product_name IS NULL) AS null_product_name_count,
  COUNTIF(category IS NULL) AS null_category_count,
  COUNTIF(price IS NULL) AS null_price_count,
  COUNTIF(stock_quantity IS NULL) AS null_stock_quantity_count
FROM
  `bq-data-ml-engineering-459914.retail_dataset.products`;

-- Check 2: Check for duplicate product_id (should be unique)
SELECT
  product_id,
  COUNT(*) AS frequency
FROM
  `bq-data-ml-engineering-459914.retail_dataset.products`
GROUP BY
  product_id
HAVING
  COUNT(*) > 1;

-- Check 3: Check for negative or zero prices (price should generally be > 0)
SELECT
  product_id,
  product_name,
  price
FROM
  `bq-data-ml-engineering-459914.retail_dataset.products`
WHERE
  price <= 0; -- Adjust if 0 is a valid price for some items (e.g., free samples)

-- Check 4: Check for negative stock quantities (stock should be >= 0)
SELECT
  product_id,
  product_name,
  stock_quantity
FROM
  `bq-data-ml-engineering-459914.retail_dataset.products`
WHERE
  stock_quantity < 0;

-- Check 5: Check for unreasonably high prices (adjust threshold as needed)
SELECT
  product_id,
  product_name,
  price
FROM
  `bq-data-ml-engineering-459914.retail_dataset.products`
WHERE
  price > 10000; -- Example threshold, adjust based on your product range

-- Check 6: Check distinct categories for potential inconsistencies (e.g., typos, casing)
SELECT
  category,
  COUNT(*) AS count_per_category
FROM
  `bq-data-ml-engineering-459914.retail_dataset.products`
GROUP BY
  category
ORDER BY
  category;


-- ***********************************************************
-- Data Quality Checks for sales_transaction Table
-- Project: bq-data-ml-engineering
-- Dataset: retail_dataset
-- Table: sales_transactions
-- ***********************************************************

-- Check 7: Count NULLs in essential columns
SELECT
  COUNTIF(transaction_id IS NULL) AS null_transaction_id_count,
  COUNTIF(store_id IS NULL) AS null_store_id_count,
  COUNTIF(product_id IS NULL) AS null_product_id_count,
  COUNTIF(quantity IS NULL) AS null_quantity_count,
  COUNTIF(transaction_date IS NULL) AS null_transaction_date_count,
  COUNTIF(total_amount IS NULL) AS null_total_amount_count
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions`;

-- Check 8: Check for duplicate transaction_id (should be unique)
SELECT
  transaction_id,
  COUNT(*) AS frequency
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions`
GROUP BY
  transaction_id
HAVING
  COUNT(*) > 1;

-- Check 9: Check for non-positive quantities (quantity should be > 0)
SELECT
  transaction_id,
  product_id,
  quantity
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions`
WHERE
  quantity <= 0;

-- Check 10: Check for negative total amounts (total_amount should be >= 0, allowing for $0 refunds/exchanges if applicable)
SELECT
  transaction_id,
  total_amount
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions`
WHERE
  total_amount < 0;

-- Check 11: Check for future transaction dates
SELECT
  transaction_id,
  transaction_date
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions`
WHERE
  transaction_date > CURRENT_DATE();

-- Check 12: Referential Integrity - Check for product_id in sales_transaction that don't exist in products
SELECT
  st.transaction_id,
  st.product_id
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions` st
LEFT JOIN
  `bq-data-ml-engineering-459914.retail_dataset.products` p
ON
  st.product_id = p.product_id
WHERE
  p.product_id IS NULL; -- This identifies sales records with invalid product IDs

-- Check 13: Referential Integrity - Check for store_id in sales_transaction that don't exist in store
SELECT
  st.transaction_id,
  st.store_id
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions` st
LEFT JOIN
  `bq-data-ml-engineering-459914.retail_dataset.stores` s
ON
  st.store_id = s.store_id
WHERE
  s.store_id IS NULL; -- This identifies sales records with invalid store IDs

-- Check 14: Consistency Check - Compare total_amount with (quantity * price)
-- Note: This might have discrepancies due to discounts, taxes, or price changes.
-- Look for significant differences. Adjust the tolerance (e.g., 0.01 for rounding).
SELECT
  st.transaction_id,
  st.quantity,
  p.price,
  st.total_amount,
  (st.quantity * p.price) AS calculated_amount,
  ABS(st.total_amount - (st.quantity * p.price)) AS difference
FROM
  `bq-data-ml-engineering-459914.retail_dataset.sales_transactions` st
JOIN
  `bq-data-ml-engineering-459914.retail_dataset.products` p
ON
  st.product_id = p.product_id
WHERE
  ABS(st.total_amount - (st.quantity * p.price)) > 0.01; -- Check for differences larger than a small tolerance


-- ***********************************************************
-- Data Quality Checks for store Table
-- Project: bq-data-ml-engineering
-- Dataset: retail_dataset
-- Table: store
-- ***********************************************************

-- Check 15: Count NULLs in key columns
SELECT
  COUNTIF(store_id IS NULL) AS null_store_id_count,
  COUNTIF(store_name IS NULL) AS null_store_name_count,
  COUNTIF(location IS NULL) AS null_location_count,
  COUNTIF(city IS NULL) AS null_city_count,
  COUNTIF(state IS NULL) AS null_state_count,
  COUNTIF(zip_code IS NULL) AS null_zip_code_count
FROM
  `bq-data-ml-engineering-459914.retail_dataset.stores`;

-- Check 16: Check for duplicate store_id (should be unique)
SELECT
  store_id,
  COUNT(*) AS frequency
FROM
  `bq-data-ml-engineering-459914.retail_dataset.stores`
GROUP BY
  store_id
HAVING
  COUNT(*) > 1;

-- Check 17: Check state format (e.g., if expecting 2-letter codes)
-- This assumes US 2-letter state codes. Adjust regex if needed.
SELECT
  store_id,
  store_name,
  state
FROM
  `bq-data-ml-engineering-459914.retail_dataset.stores`
WHERE
  LENGTH(state) != 2 OR NOT REGEXP_CONTAINS(state, r'^[A-Z]{2}$'); -- Checks for exactly 2 uppercase letters

-- Check 18: Check zip code format (e.g., if expecting 5 digits)
-- This assumes US 5-digit zip codes. Adjust regex if needed (e.g., for ZIP+4).
SELECT
  store_id,
  store_name,
  zip_code
FROM
  `bq-data-ml-engineering-459914.retail_dataset.stores`
WHERE
  CAST(zip_code AS STRING) IS NULL -- Check if conversion fails (shouldn't for INTEGER)
  OR NOT REGEXP_CONTAINS(CAST(zip_code AS STRING), r'^[0-9]{5}$'); -- Checks for exactly 5 digits

-- Check 19: Check distinct states for potential inconsistencies (typos, casing)
SELECT
  state,
  COUNT(*) AS count_per_state
FROM
  `bq-data-ml-engineering-459914.retail_dataset.stores`
GROUP BY
  state
ORDER BY
  state;

-- Check 20: Check distinct cities for potential inconsistencies (typos, casing)
SELECT
  city,
  COUNT(*) AS count_per_city
FROM
  `bq-data-ml-engineering-459914.retail_dataset.stores`
GROUP BY
  city
ORDER BY
  city;