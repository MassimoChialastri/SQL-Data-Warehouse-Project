/*
===============================================================================
Stored Procedure: Load Gold Layer (Silver -> Gold)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'gold' schema tables from the 'silver' schema.
	Actions Performed:
		- Truncates Gold tables.
		- Inserts transformed and cleansed data from Silver into Gold tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC gold.load_gold;
===============================================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Gold Layer';
        PRINT '================================================';

		-- Loading gold.dim_customer
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.dim_customer';
		TRUNCATE TABLE gold.dim_customer;
		PRINT '>> Inserting Data Into: gold.dim_customer';
		INSERT INTO gold.dim_customer (
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state,
			customer_latitude,
			customer_longitude,
			customer_start_date,
			customer_end_date
		)
		SELECT 
			c.customer_id,
			c.customer_unique_id,
			c.customer_zip_code_prefix,
			c.customer_city,
			c.customer_state,
			g.geolocation_lat AS customer_latitude,
			g.geolocation_lng AS customer_longitude,
			CAST(o.order_purchase_timestamp AS DATE) AS customer_start_date,
			CASE WHEN CAST(o.order_purchase_timestamp AS DATE) = 
					  LEAD(CAST(o.order_purchase_timestamp AS DATE)) OVER (
						PARTITION BY c.customer_unique_id ORDER BY CAST(o.order_purchase_timestamp AS DATE))
				 THEN CAST(o.order_purchase_timestamp AS DATE)
				 ELSE 
					DATEADD(DAY, -1, LEAD(CAST(o.order_purchase_timestamp AS DATE)) OVER (
					PARTITION BY customer_unique_id ORDER BY CAST(o.order_purchase_timestamp AS DATE))) 
			END AS customer_end_date
		FROM silver.customers c
		LEFT JOIN silver.orders o
		ON c.customer_id = o.customer_id
		LEFT JOIN silver.geolocation g
		ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix AND 
		   c.customer_city = g.geolocation_city AND
		   c.customer_state = g.geolocation_state
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading gold.dim_product
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.dim_product';
		TRUNCATE TABLE gold.dim_product;
		PRINT '>> Inserting Data Into: gold.dim_product';
		INSERT INTO gold.dim_product (
			product_id,
			product_category_name,
			product_name_length,
			product_description_length,
			product_photos_quantity,
			product_weight_grams,
			product_length_cm,
			product_height_cm,
			product_width_cm
		)
		SELECT 
			product_id,
			product_category_name,
			product_name_length,
			product_description_length,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm,
			product_width_cm
		FROM silver.products
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading gold.dim_seller
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.dim_seller';
		TRUNCATE TABLE gold.dim_seller;
		PRINT '>> Inserting Data Into: gold.dim_seller';
		INSERT INTO gold.dim_seller (
			seller_id,
			seller_zip_code_prefix,
			seller_city,
			seller_state,
			seller_latitude,
			seller_longitude
		)
		SELECT 
			s.seller_id,
			s.seller_zip_code_prefix,
			s.seller_city,
			s.seller_state,
			g.geolocation_lat,
			g.geolocation_lng
		FROM silver.sellers s
		LEFT JOIN silver.geolocation g
		ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix AND
		   s.seller_city = g.geolocation_city AND
		   s.seller_state = g.geolocation_state
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading gold.fact_orders
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.fact_orders';
		TRUNCATE TABLE gold.fact_orders;
		PRINT '>> Inserting Data Into: gold.fact_orders';
		INSERT INTO gold.fact_orders (
			order_id,
			customer_key,
			order_items_number,
			order_payment,
			payment_methods_number,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date
		)
		SELECT 
			o.order_id,
			c.customer_key,
			i.order_items_number,
			p.payment_tot_value AS order_payment,
			p.payment_methods_number,
			o.order_status,
			o.order_purchase_timestamp,
			o.order_approved_at,
			o.order_delivered_carrier_date,
			o.order_delivered_customer_date,
			o.order_estimated_delivery_date
		FROM silver.orders o 
		LEFT JOIN gold.dim_customer c
		ON o.customer_id = c.customer_id
		LEFT JOIN (
			SELECT DISTINCT 
				order_id,
				payment_methods_number,
				payment_tot_value 
			FROM silver.order_payments
		) p
		ON o.order_id = p.order_id
		LEFT JOIN (
		SELECT DISTINCT 
			order_id,
			order_items_number
		FROM silver.order_items
		) i
		ON o.order_id = i.order_id
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading gold.fact_order_reviews
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.fact_order_reviews';
		TRUNCATE TABLE gold.fact_order_reviews;
		PRINT '>> Inserting Data Into: gold.fact_order_reviews';
		INSERT INTO gold.fact_order_reviews (
			review_id,
			order_key,
			review_score,
			review_comment_title,
			review_comment_message,
			review_survey_creation_date,
			review_answer_timestamp
		)
		SELECT
			r.review_id,
			o.order_key,
			r.review_score,
			r.review_comment_title,
			r.review_comment_message,
			r.review_creation_date AS review_survey_creation_date,
			r.review_answer_timestamp
		FROM silver.order_reviews AS r
		LEFT JOIN gold.fact_orders AS o
		ON r.order_id = o.order_id
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading gold.fact_order_items
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.fact_order_items';
		TRUNCATE TABLE gold.fact_order_items;
		PRINT '>> Inserting Data Into: gold.fact_order_items';
		INSERT INTO gold.fact_order_items (
			order_key,
			order_item_number,
			product_key,
			seller_key,
			price,
			freight_value,
			shipping_limit_date
		)
		SELECT 
			o.order_key,
			i.order_item_id AS order_item_number,
			p.product_key,
			s.seller_key,
			i.price,
			i.freight_value,
			i.shipping_limit_date
		FROM silver.order_items i
		LEFT JOIN gold.fact_orders o
		ON i.order_id = o.order_id
		LEFT JOIN gold.dim_product p
		ON i.product_id = p.product_id
		LEFT JOIN gold.dim_seller s
		ON  i.seller_id = s.seller_id
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading gold.fact_order_payments
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.fact_order_payments';
		TRUNCATE TABLE gold.fact_order_payments;
		PRINT '>> Inserting Data Into: gold.fact_order_payments';
		INSERT INTO gold.fact_order_payments (
			order_key,
			payment_sequential,
			payment_type,
			payment_installments,
			payment_value
		)
		SELECT
			o.order_key,
			p.payment_sequential,
			p.payment_type,
			p.payment_installments,
			p.payment_value
		FROM silver.order_payments p
		LEFT JOIN gold.fact_orders o
		ON p.order_id = o.order_id
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		-- Loading gold.dim_date
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: gold.dim_date';
		TRUNCATE TABLE gold.dim_date;
		PRINT '>> Inserting Data Into: gold.dim_date';
		WITH cte_date_series AS (
			SELECT CAST('2016-01-01' AS DATE) AS date_value

			UNION ALL

			SELECT DATEADD(DAY, 1, date_value)
			FROM cte_date_series
			WHERE date_value < '2020-12-31'
		)
		INSERT INTO gold.dim_date (
		date, 
		year, 
		quarter, 
		month_number, 
		month, 
		day,
		day_name
		)
		SELECT
			date_value AS date,
			YEAR(date_value) AS year,
			DATEPART(QUARTER, date_value) AS quarter,
			MONTH(date_value) AS month_number,
			DATENAME(MONTH, date_value) AS month,
			DAY(date_value) AS day,
			DATENAME(WEEKDAY, date_value) AS day_name
		FROM cte_date_series
		OPTION (MAXRECURSION 2000);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Gold Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING GOLD LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END