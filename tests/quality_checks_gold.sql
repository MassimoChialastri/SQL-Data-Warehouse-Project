/*
===============================================================================
Quality Checks – Gold Layer
===============================================================================
Script Purpose:
    This script performs quality checks on the gold layer of the data warehouse.
    Checks are organised by table and cover:
    - Null or duplicate surrogate/natural keys.
    - Invalid or missing foreign key references.
    - Data consistency between related fields.
    - Invalid metric values (negative amounts, scores out of range, etc.).
    - SCD validity (dim_customer start/end dates).
    - Date dimension completeness and internal consistency.

Usage Notes:
    - Run these checks after loading the gold layer from the silver layer.
    - Investigate and resolve any discrepancies before exposing the gold layer
      to BI tools or downstream consumers.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customer'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT
    customer_key,
    COUNT(*)
FROM gold.dim_customer
GROUP BY customer_key
HAVING COUNT(*) > 1 OR customer_key IS NULL;

-- Check for NULLs or invalid customer_id (natural key, 32 chars)
-- Expectation: No Results
SELECT customer_id
FROM gold.dim_customer
WHERE customer_id IS NULL OR LEN(customer_id) < 32;

-- Check for NULLs or invalid customer_unique_id
-- Expectation: No Results
SELECT customer_unique_id
FROM gold.dim_customer
WHERE customer_unique_id IS NULL OR LEN(customer_unique_id) < 32;

-- Check for invalid customer_zip_code_prefix
-- Expectation: No Results
SELECT customer_zip_code_prefix
FROM gold.dim_customer
WHERE LEN(customer_zip_code_prefix) < 5;

-- Check for invalid customer_state (must be at least 2 chars)
-- Expectation: No Results
SELECT customer_state
FROM gold.dim_customer
WHERE LEN(customer_state) < 2;

-- Check for zip_code_prefix mapped to multiple states
-- Expectation: No Results
SELECT
    customer_zip_code_prefix,
    COUNT(DISTINCT customer_state)
FROM gold.dim_customer
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_state) > 1;

-- Check SCD validity: customer_start_date must precede customer_end_date
-- Expectation: No Results
SELECT *
FROM gold.dim_customer
WHERE customer_end_date IS NOT NULL
  AND customer_start_date > customer_end_date;

-- Check that customer_start_date is never NULL
-- Expectation: No Results
SELECT *
FROM gold.dim_customer
WHERE customer_start_date IS NULL;

-- Check for unwanted spaces in string fields
-- Expectation: No Results
SELECT *
FROM gold.dim_customer
WHERE TRIM(customer_id)              != customer_id
   OR TRIM(customer_unique_id)       != customer_unique_id
   OR TRIM(customer_zip_code_prefix) != customer_zip_code_prefix
   OR TRIM(customer_city)            != customer_city
   OR TRIM(customer_state)           != customer_state;


-- ====================================================================
-- Checking 'gold.dim_seller'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT
    seller_key,
    COUNT(*)
FROM gold.dim_seller
GROUP BY seller_key
HAVING COUNT(*) > 1 OR seller_key IS NULL;

-- Check for NULLs or invalid seller_id (natural key, 32 chars)
-- Expectation: No Results
SELECT seller_id
FROM gold.dim_seller
WHERE seller_id IS NULL OR LEN(seller_id) < 32;

-- Check for invalid seller_zip_code_prefix
-- Expectation: No Results
SELECT seller_zip_code_prefix
FROM gold.dim_seller
WHERE LEN(seller_zip_code_prefix) < 5;

-- Check for invalid seller_state
-- Expectation: No Results
SELECT seller_state
FROM gold.dim_seller
WHERE LEN(seller_state) < 2;

-- Check for zip_code_prefix mapped to multiple states
-- Expectation: No Results
SELECT
    seller_zip_code_prefix,
    COUNT(DISTINCT seller_state)
FROM gold.dim_seller
GROUP BY seller_zip_code_prefix
HAVING COUNT(DISTINCT seller_state) > 1
   AND seller_zip_code_prefix IS NOT NULL;

-- Check for unwanted spaces in string fields
-- Expectation: No Results
SELECT *
FROM gold.dim_seller
WHERE TRIM(seller_id)              != seller_id
   OR TRIM(seller_zip_code_prefix) != seller_zip_code_prefix
   OR TRIM(seller_city)            != seller_city
   OR TRIM(seller_state)           != seller_state;


-- ====================================================================
-- Checking 'gold.dim_product'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT
    product_key,
    COUNT(*)
FROM gold.dim_product
GROUP BY product_key
HAVING COUNT(*) > 1 OR product_key IS NULL;

-- Check for NULLs or invalid product_id (natural key, 32 chars)
-- Expectation: No Results
SELECT product_id
FROM gold.dim_product
WHERE product_id IS NULL OR LEN(product_id) < 32;

-- Validate that product metrics are strictly positive
-- Expectation: No Results
SELECT *
FROM gold.dim_product
WHERE product_name_length        <= 0
   OR product_description_length <= 0
   OR product_photos_quantity    <= 0
   OR product_weight_grams        < 0
   OR product_length_cm          <= 0
   OR product_height_cm          <= 0;

-- Check for unwanted spaces in string fields
-- Expectation: No Results
SELECT *
FROM gold.dim_product
WHERE TRIM(product_id)            != product_id
   OR TRIM(product_category_name) != product_category_name;


-- ====================================================================
-- Checking 'gold.dim_date'
-- ====================================================================

-- Check for NULLs or duplicates in the primary key
-- Expectation: No Results
SELECT
    date,
    COUNT(*)
FROM gold.dim_date
GROUP BY date
HAVING COUNT(*) > 1 OR date IS NULL;

-- Check that year, quarter, month_number, day are consistent with the date column
-- Expectation: No Results
SELECT *
FROM gold.dim_date
WHERE YEAR(date)       != year
   OR MONTH(date)      != month_number
   OR DAY(date)        != day
   OR DATEPART(dw, date) != CASE day_name
        WHEN 'Monday'    THEN 2
        WHEN 'Tuesday'   THEN 3
        WHEN 'Wednesday' THEN 4
        WHEN 'Thursday'  THEN 5
        WHEN 'Friday'    THEN 6
        WHEN 'Saturday'  THEN 7
        WHEN 'Sunday'    THEN 1
        ELSE NULL END;

-- Check that quarter is consistent with month_number
-- Expectation: No Results
SELECT *
FROM gold.dim_date
WHERE quarter != CEILING(month_number / 3.0);

-- Check that month label is consistent with month_number (if a month column exists)
-- Expectation: No Results
SELECT *
FROM gold.dim_date
WHERE month_number != MONTH(CAST('2000-' + month + '-01' AS DATE));

-- Check for missing dates (no gaps in the date sequence)
-- Expectation: No Results
SELECT
    DATEADD(day, 1, d1.date) AS missing_date
FROM gold.dim_date d1
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.dim_date d2
    WHERE d2.date = DATEADD(day, 1, d1.date)
)
AND DATEADD(day, 1, d1.date) <= (SELECT MAX(date) FROM gold.dim_date);


-- ====================================================================
-- Checking 'gold.fact_orders'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT
    order_key,
    COUNT(*)
FROM gold.fact_orders
GROUP BY order_key
HAVING COUNT(*) > 1 OR order_key IS NULL;

-- Check for NULLs or invalid order_id (natural key, 32 chars)
-- Expectation: No Results
SELECT order_id
FROM gold.fact_orders
WHERE order_id IS NULL OR LEN(order_id) < 32;

-- Check FK integrity: customer_key must exist in dim_customer
-- Expectation: No Results
SELECT order_key
FROM gold.fact_orders
WHERE customer_key NOT IN (SELECT customer_key FROM gold.dim_customer);

-- Check FK integrity: order_purchase_timestamp must exist in dim_date
-- Expectation: No Results
SELECT order_key
FROM gold.fact_orders
WHERE CAST(order_purchase_timestamp AS DATE) NOT IN (SELECT date FROM gold.dim_date);

-- Check for NULL order_purchase_timestamp
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE order_purchase_timestamp IS NULL;

-- Check order_items_number is positive
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE order_items_number <= 0;

-- Check payment_methods_number is positive
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE payment_methods_number <= 0;

-- Check order_payment (total amount) is positive
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE order_payment <= 0;

-- Data standardisation: inspect distinct order statuses
SELECT DISTINCT order_status FROM gold.fact_orders;

-- Check date ordering:
-- order_purchase_timestamp < order_approved_at < order_delivered_carrier_date
--   < order_delivered_customer_date
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
EXCEPT
SELECT *
FROM gold.fact_orders
WHERE (order_purchase_timestamp <= order_approved_at OR order_approved_at IS NULL) AND
      (order_approved_at <= order_delivered_carrier_date OR order_approved_at IS NULL OR order_delivered_carrier_date IS NULL) AND
      (order_delivered_carrier_date <= order_delivered_customer_date OR order_delivered_carrier_date IS NULL OR order_delivered_customer_date IS NULL) AND
      (order_delivered_carrier_date <= order_estimated_delivery_date OR order_delivered_carrier_date IS NULL OR order_estimated_delivery_date IS NULL);

-- Check status-specific date constraints (mirrors silver checks)
-- 'approved' orders must not have carrier/customer delivery dates
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE order_status = 'approved'
  AND (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL);

-- 'shipped' orders must not have customer delivery date
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE order_status = 'shipped'
  AND order_delivered_customer_date IS NOT NULL;

-- 'created' / 'invoiced' / 'processing' / 'unavailable' / 'canceled' orders
-- must not have carrier or customer delivery dates
-- Expectation: No Results
SELECT *
FROM gold.fact_orders
WHERE order_status IN ('created', 'invoiced', 'processing', 'unavailable', 'canceled')
  AND (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL);


-- ====================================================================
-- Checking 'gold.fact_order_items'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT
    order_items_key,
    COUNT(*)
FROM gold.fact_order_items
GROUP BY order_items_key
HAVING COUNT(*) > 1 OR order_items_key IS NULL;

-- Check FK integrity: order_key must exist in fact_orders
-- Expectation: No Results
SELECT order_items_key
FROM gold.fact_order_items
WHERE order_key NOT IN (SELECT order_key FROM gold.fact_orders);

-- Check FK integrity: product_key must exist in dim_product
-- Expectation: No Results
SELECT order_items_key
FROM gold.fact_order_items
WHERE product_key NOT IN (SELECT product_key FROM gold.dim_product);

-- Check FK integrity: seller_key must exist in dim_seller
-- Expectation: No Results
SELECT order_items_key
FROM gold.fact_order_items
WHERE seller_key NOT IN (SELECT seller_key FROM gold.dim_seller);

-- Check order_item_number is positive
-- Expectation: No Results
SELECT *
FROM gold.fact_order_items
WHERE order_item_number IS NULL OR order_item_number <= 0;

-- Check price is positive and not NULL
-- Expectation: No Results
SELECT *
FROM gold.fact_order_items
WHERE price IS NULL OR price <= 0;

-- Check freight_value is non-negative and not NULL
-- Expectation: No Results
SELECT *
FROM gold.fact_order_items
WHERE freight_value IS NULL OR freight_value < 0;

-- Check shopping_limit_date is not suspiciously old
-- Expectation: No Results
SELECT *
FROM gold.fact_order_items
WHERE shipping_limit_date IS NULL
   OR shipping_limit_date < '2000-01-01';

-- Check shopping_limit_date must be >= order purchase date (via fact_orders join)
-- Expectation: No Results
SELECT i.order_items_key
FROM gold.fact_order_items i
JOIN gold.fact_orders o ON i.order_key = o.order_key
WHERE i.shipping_limit_date < o.order_purchase_timestamp;


-- ====================================================================
-- Checking 'gold.fact_order_payments'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT
    order_payment_key,
    COUNT(*)
FROM gold.fact_order_payments
GROUP BY order_payment_key
HAVING COUNT(*) > 1 OR order_payment_key IS NULL;

-- Check FK integrity: order_key must exist in fact_orders
-- Expectation: No Results
SELECT order_payment_key
FROM gold.fact_order_payments
WHERE order_key NOT IN (SELECT order_key FROM gold.fact_orders);

-- Check for NULLs in payment_sequential
-- Expectation: No Results
SELECT *
FROM gold.fact_order_payments
WHERE payment_sequential IS NULL;

-- Check composite natural key (order_key + payment_sequential) has no duplicates
-- Expectation: No Results
SELECT
    order_key,
    payment_sequential,
    COUNT(*)
FROM gold.fact_order_payments
GROUP BY order_key, payment_sequential
HAVING COUNT(*) > 1;

-- Data standardisation: inspect distinct payment types
SELECT DISTINCT payment_type FROM gold.fact_order_payments;

-- Check payment_installments is positive
-- Expectation: No Results
SELECT *
FROM gold.fact_order_payments
WHERE payment_installments <= 0;

-- Check payment_value is positive
-- Expectation: No Results
SELECT *
FROM gold.fact_order_payments
WHERE payment_value <= 0;

-- Cross-check: sum of payment_value per order must match order_payment in fact_orders
-- Tolerance of 0.01 to account for rounding
-- Expectation: No Results
SELECT
    p.order_key,
    SUM(p.payment_value)    AS sum_payments,
    o.order_payment         AS order_total
FROM gold.fact_order_payments p
JOIN gold.fact_orders o ON p.order_key = o.order_key
GROUP BY p.order_key, o.order_payment
HAVING ABS(SUM(p.payment_value) - o.order_payment) > 0.01;

-- Check for unwanted spaces in payment_type
-- Expectation: No Results
SELECT *
FROM gold.fact_order_payments
WHERE TRIM(payment_type) != payment_type;

-- ====================================================================
-- Checking 'gold.fact_order_reviews'
-- ====================================================================

-- Check for NULLs or duplicates in surrogate key
-- Expectation: No Results
SELECT
    review_key,
    COUNT(*)
FROM gold.fact_order_reviews
GROUP BY review_key
HAVING COUNT(*) > 1 OR review_key IS NULL;

-- Check for NULLs or invalid review_id (natural key, 32 chars)
-- Expectation: No Results
SELECT review_id
FROM gold.fact_order_reviews
WHERE review_id IS NULL OR LEN(review_id) < 32;

-- Check FK integrity: order_key must exist in fact_orders
-- Expectation: No Results
SELECT review_key
FROM gold.fact_order_reviews
WHERE order_key NOT IN (SELECT order_key FROM gold.fact_orders);

-- Check review_score is in valid range [1, 5]
-- Expectation: No Results
SELECT *
FROM gold.fact_order_reviews
WHERE review_score IS NULL
   OR review_score < 1
   OR review_score > 5;

-- Validate that review_survey_creation precedes review_answer_timestamp
-- Expectation: No Results
SELECT *
FROM gold.fact_order_reviews
WHERE review_survey_creation_date > review_answer_timestamp;

-- Validate that review_survey_creation is after the order purchase date
-- Expectation: No Results
SELECT r.review_key
FROM gold.fact_order_reviews r
JOIN gold.fact_orders o ON r.order_key = o.order_key
WHERE r.review_survey_creation_date < o.order_purchase_timestamp;

-- Check for unwanted spaces in text fields
-- Expectation: No Results
SELECT *
FROM gold.fact_order_reviews
WHERE TRIM(review_id)              != review_id
   OR TRIM(review_comment_title)   != review_comment_title
   OR TRIM(review_comment_message) != review_comment_message;
