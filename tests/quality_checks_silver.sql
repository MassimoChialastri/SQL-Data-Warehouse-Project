/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.customers'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
customer_id,
customer_unique_id,
customer_city,
customer_state
FROM silver.customers
WHERE TRIM(customer_id) != customer_id OR 
      TRIM(customer_unique_id) != customer_unique_id OR
	  TRIM(customer_zip_code_prefix) != customer_zip_code_prefix OR
	  TRIM(customer_city) != customer_city OR
	  TRIM(customer_state) != customer_state

-- Check for NULLs, Duplicates or Invalid values in Primary Key
-- Expectation: No Results
SELECT
	customer_id,
	COUNT(*)
FROM silver.customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL OR LEN(customer_id) < 32

-- Check for NULLs or Invalid values in customer_unique_id
-- Expectation: No Results
SELECT
	customer_unique_id
FROM silver.customers
WHERE customer_unique_id IS NULL OR LEN(customer_unique_id) < 32

-- Check for Invalid values in customer_zip_code_prefix
-- Expectation: No Results
SELECT
	customer_zip_code_prefix
FROM silver.customers
WHERE LEN(customer_zip_code_prefix) < 5


-- Check for Invalid values in customer_state
-- Expectation: No Results
SELECT
	customer_state
FROM silver.customers
WHERE LEN(customer_state) < 2

-- Check for customer_zip_code_prefix with multiple values of customer_state
-- Expectation: No Results
SELECT 
    customer_zip_code_prefix,
    COUNT(DISTINCT customer_state) 
FROM silver.customers
GROUP BY customer_zip_code_prefix
HAVING COUNT(DISTINCT customer_state) > 1;

-- ====================================================================
-- Checking 'silver.geolocation'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	geolocation_zip_code_prefix,
	geolocation_city,
	geolocation_state
FROM silver.geolocation
WHERE TRIM(geolocation_zip_code_prefix) != geolocation_zip_code_prefix OR 
      TRIM(geolocation_city) != geolocation_city OR
	  TRIM(geolocation_state) != geolocation_state

-- Check for Invalid values in geolocation_zip_code_prefix
-- Expectation: No Results
SELECT
	geolocation_zip_code_prefix
FROM silver.geolocation
WHERE LEN(geolocation_zip_code_prefix) < 5

-- Check for Invalid values in geolocation_state
-- Expectation: No Results
SELECT
	geolocation_state
FROM silver.geolocation
WHERE LEN(geolocation_state) < 2

-- Check for customer_zip_code_prefix with multiple values of customer_state
-- Expectation: No Results 
SELECT 
    geolocation_zip_code_prefix,
    COUNT(DISTINCT geolocation_state) 
FROM silver.geolocation
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(DISTINCT geolocation_state) > 1 AND geolocation_zip_code_prefix IS NOT NULL;

-- ====================================================================
-- Checking 'silver.order_items'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT *
FROM silver.order_items
WHERE TRIM(order_id) != order_id OR 
      TRIM(product_id) != product_id OR
	  TRIM(seller_id) != seller_id 

-- Check for NULLs or invalid FKs
-- Expectation: No Results
SELECT
	order_id,
	product_id,
	seller_id
FROM silver.order_items
WHERE order_id IS NULL OR LEN(order_id) < 32 OR
      product_id IS NULL OR LEN(product_id) < 32 OR
	  seller_id IS NULL OR LEN(seller_id) < 32

-- Check if there are order_id not present in silver.orders
-- Expectation: No Results
SELECT order_id
FROM silver.order_items
WHERE order_id NOT IN (SELECT order_id FROM silver.orders)

-- Check if there are product_id not present in silver.products
-- Expectation: No Results
SELECT product_id
FROM silver.order_items
WHERE product_id NOT IN (SELECT product_id FROM bronze.products)

-- Check if there are seller_id not present in silver.sellers
-- Expectation: No Results
SELECT seller_id
FROM silver.order_items
WHERE seller_id NOT IN (SELECT seller_id FROM silver.sellers)

-- Check if there are items with:
-- 1) NULL or smaller or equal to 0 price
-- 2) Null or negative freight_value
-- Expectation: No Results
SELECT *
FROM silver.order_items
WHERE price <= 0 OR freight_value < 0 OR price IS NULL OR freight_value IS NULL

-- Check for duplicate primary keys
-- Expectation: No Results
SELECT order_id, order_item_id, COUNT(*)
FROM silver.order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1

-- Check for invalid or suspiciously old shipping dates
-- Expectation: No Results
SELECT *
FROM bronze.order_items
WHERE shipping_limit_date IS NULL
   OR shipping_limit_date < '2000-01-01'

-- Check order_item_id is positive
-- Expectation: No Results
SELECT *
FROM bronze.order_items
WHERE order_item_id IS NULL OR order_item_id <= 0

-- ====================================================================
-- Checking 'silver.order_payments'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT *
FROM silver.order_payments
WHERE TRIM(order_id) != order_id OR 
      TRIM(payment_type) != payment_type 

-- Check for NULLs or invalid order_id
-- Expectation: No Results
SELECT
	order_id
FROM silver.order_payments
WHERE order_id IS NULL OR LEN(order_id) < 32

-- Check for Duplicates in PK
-- Expectation: No Results
SELECT
	order_id,
	payment_sequential,
	COUNT(*)
FROM silver.order_payments
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1

-- Data Standardization & Consistency
SELECT DISTINCT
	payment_type
FROM silver.order_payments

-- Check for zero or negative values of payment_installments
-- Expectation: No Results
SELECT 
	order_id,
	payment_installments
FROM silver.order_payments
WHERE payment_installments <= 0

-- Check for zero or negative values of payment_value
-- Expectation: No Results
SELECT 
	order_id,
	payment_value
FROM silver.order_payments
WHERE payment_value <= 0

-- If any payment_value of an order is NULL then 
-- tot_payment_value must be NULL
-- Expectation: No Results
SELECT *
FROM (
	SELECT 
		order_id,
		payment_sequential,
		payment_value,
		tot_payment_value,
		SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END)
			OVER (PARTITION BY order_id) AS flag_null
	FROM silver.order_payments
) t
WHERE flag_null > 0 AND tot_payment_value IS NOT NULL

-- ====================================================================
-- Checking 'silver.order_reviews'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT *
FROM silver.order_reviews
WHERE TRIM(review_id) != review_id OR 
      TRIM(order_id) != order_id OR
	  TRIM(review_comment_title) != review_comment_title OR
	  TRIM(review_comment_message) != review_comment_message

-- Check for NULLs or invalid review_id
-- Expectation: No Results
SELECT
	review_id
FROM silver.order_reviews
WHERE review_id IS NULL OR LEN(review_id) < 32

-- Check for NULLs or invalid order_id
-- Expectation: No Results
SELECT
	order_id
FROM silver.order_reviews
WHERE order_id IS NULL OR LEN(order_id) < 32

-- Validate that review_creation_date always precedes review_answer_timestamp
-- Expectation: No Results
SELECT *
FROM silver.order_reviews
WHERE review_creation_date > review_answer_timestamp

-- ====================================================================
-- Checking 'silver.orders'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	order_id,
	customer_id
	order_status
FROM silver.orders
WHERE TRIM(order_id) != order_id OR 
      TRIM(customer_id) != customer_id OR
	  TRIM(order_status) != order_status 

-- Check for NULLs, Duplicates or Invalid values in Primary Key
-- Expectation: No Results
SELECT
	order_id,
	COUNT(*)
FROM silver.orders
GROUP BY order_id
HAVING COUNT(*) > 1 OR order_id IS NULL OR LEN(order_id) < 32

-- Check for NULLs or Invalid values in Foreign Key
-- Expectation: No Results
SELECT
	customer_id
FROM silver.orders
WHERE customer_id IS NULL OR LEN(customer_id) < 32

-- Data Standardization & Consistency
SELECT DISTINCT
	order_status
FROM silver.orders

-- Check if there are orders with NULL order_purchase_timestamp 
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_purchase_timestamp IS NULL

-- Check for 'approved' orders with order_delivered_carrier_date 
-- or order_delivered_customer_date not NULL
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_status = 'approved' AND
      (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL)

-- Check for 'created' orders with order_delivered_carrier_date 
-- or order_delivered_customer_date not NULL
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_status = 'created' AND
      (order_approved_at IS NOT NULL OR
	   order_delivered_carrier_date IS NOT NULL OR
	   order_delivered_customer_date IS NOT NULL)

-- Check for 'invoiced' orders with order_delivered_carrier_date 
-- or order_delivered_customer_date not NULL
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_status = 'invoiced' AND
      (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL)

-- Check for 'processing' orders with order_delivered_carrier_date 
-- or order_delivered_customer_date not NULL
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_status = 'processing' AND 
      (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL)

-- Check for 'unavailable' orders with order_delivered_carrier_date 
-- or order_delivered_customer_date not NULL
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_status = 'unavailable' AND (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL)

-- Check for 'canceled' orders with order_delivered_carrier_date 
-- or order_delivered_customer_date not NULL
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_status = 'canceled' AND 
      (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL)

-- Check for 'shipped' orders with order_delivered_customer_date not NULL
-- Expectation: No Results
SELECT *
FROM silver.orders
WHERE order_status = 'shipped' AND
      order_delivered_customer_date IS NOT NULL

-- Check if there are orders that don't respect the following rules:
-- 1) order_purchase_timestamp < order_approved_at < order_delivered_carrier_date < order_delivered_customer_date
-- 2) order_purchase_timestamp < order_approved_at < order_delivered_carrier_date < order_estimated_delivery_date
-- Expectation: No Results
SELECT *
FROM silver.orders
EXCEPT
SELECT *
FROM silver.orders
WHERE (order_purchase_timestamp < order_approved_at OR order_purchase_timestamp IS NULL OR order_approved_at IS NULL) AND
      (order_approved_at < order_delivered_carrier_date OR order_approved_at IS NULL OR order_delivered_carrier_date IS NULL) AND
	  (order_delivered_carrier_date < order_delivered_customer_date OR order_delivered_carrier_date IS NULL OR order_delivered_customer_date IS NULL) AND
	  (order_delivered_carrier_date < order_estimated_delivery_date OR order_delivered_carrier_date IS NULL OR order_estimated_delivery_date IS NULL)

-- ====================================================================
-- Checking 'silver.product_category_name_translation'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT *
FROM silver.product_category_name_translation
WHERE TRIM(product_category_name) != product_category_name OR 
      TRIM(product_category_name_english) != product_category_name_english

-- ====================================================================
-- Checking 'silver.products'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT *
FROM silver.products
WHERE TRIM(product_id) != product_id OR 
      TRIM(product_category_name) != product_category_name

-- Check for NULLs or invalid product_id
-- Expectation: No Results
SELECT
	product_id
FROM silver.products
WHERE product_id IS NULL OR LEN(product_id) < 32

-- Validate that product measures and lengths are strictly positive
-- Expectation: No Results
SELECT *
FROM silver.products
WHERE 
	product_name_length <= 0 OR
	product_description_length <= 0 OR
	product_photos_qty <= 0 OR
	product_weight_g < 0 OR
	product_length_cm <= 0 OR
	product_height_cm <= 0

-- ====================================================================
-- Checking 'silver.sellers'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT
	seller_id,
	seller_zip_code_prefix,
	seller_city,
	seller_state
FROM silver.sellers
WHERE TRIM(seller_id) != seller_id OR 
      TRIM(seller_zip_code_prefix) != seller_zip_code_prefix OR
	  TRIM(seller_city) != seller_city OR
	  TRIM(seller_state) != seller_state

-- Check for Invalid values in seller_zip_code_prefix
-- Expectation: No Results
SELECT
	seller_zip_code_prefix
FROM silver.sellers
WHERE LEN(seller_zip_code_prefix) < 5

-- Check for Invalid values in seller_state
-- Expectation: No Results
SELECT
	seller_state
FROM silver.sellers
WHERE LEN(seller_state) < 2

-- Check for seller_zip_code_prefix with multiple values seller_state
-- Expectation: No Results 
SELECT 
    seller_zip_code_prefix,
    COUNT(DISTINCT seller_state) 
FROM silver.sellers
GROUP BY seller_zip_code_prefix
HAVING COUNT(DISTINCT seller_state) > 1 AND seller_zip_code_prefix IS NOT NULL;
