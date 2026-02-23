/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		-- Loading silver.customers
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.customers';
		TRUNCATE TABLE silver.customers;
		PRINT '>> Inserting Data Into: silver.customers';
		INSERT INTO silver.customers (
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			customer_city,
			customer_state
		)
		SELECT
			customer_id,
			customer_unique_id,
			customer_zip_code_prefix,
			silver.clean_names_fn(customer_city),
			UPPER(customer_state)
		FROM bronze.customers
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.geolocation
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.geolocation';
		TRUNCATE TABLE silver.geolocation;
		PRINT '>> Inserting Data Into: silver.geolocation';
		INSERT INTO silver.geolocation (
			geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			geolocation_city,
			geolocation_state
		)
		SELECT 
			CASE 
				WHEN geolocation_zip_code_prefix = '72915' AND silver.clean_names_fn(geolocation_city) != 'aguas lindas de goias' THEN NULL
				WHEN geolocation_zip_code_prefix = '80630' AND silver.clean_names_fn(geolocation_city) != 'curitiba' THEN NULL
				WHEN geolocation_zip_code_prefix = '78557' AND silver.clean_names_fn(geolocation_city) != 'sinop' THEN NULL
				ELSE geolocation_zip_code_prefix
			END AS geolocation_zip_code_prefix,
			geolocation_lat,
			geolocation_lng,
			silver.clean_names_fn(geolocation_city),
			CASE
				WHEN geolocation_zip_code_prefix IN ('02116', '04011') THEN 'SP'
				WHEN geolocation_zip_code_prefix IN ('21550', '23056') THEN 'RJ'
				WHEN geolocation_zip_code_prefix = '79750' THEN 'MS'
				ELSE geolocation_state
			END AS geolocation_state
		FROM bronze.geolocation
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.order_items
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.order_items';
		TRUNCATE TABLE silver.order_items;
		PRINT '>> Inserting Data Into: silver.order_items';
		INSERT INTO silver.order_items (
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date,
			price,
			freight_value,
			total_price,
			total_freight_value
		)
		SELECT
			order_id,
			order_item_id,
			product_id,
			seller_id,
			shipping_limit_date,
			price,
			freight_value,
			SUM(price) OVER (PARTITION BY order_id),
			SUM(freight_value) OVER (PARTITION BY order_id)
		FROM bronze.order_items
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.order_payments
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.order_payments';
		TRUNCATE TABLE silver.order_payments;
		PRINT '>> Inserting Data Into: silver.order_payments';
		INSERT INTO silver.order_payments (
			order_id,
			payment_sequential,
			payment_type,
			payment_installments,
			payment_value,
			tot_payment_value
		)
		SELECT
			order_id,
			ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_sequential) AS payment_sequential,
			CASE payment_type
				WHEN 'boleto' THEN 'bank slip'
				WHEN 'not_defined' THEN 'n/a'
				ELSE REPLACE(payment_type, '_', ' ')
			END AS payment_type,
			CASE WHEN payment_installments <= 0 THEN NULL
				 ELSE payment_installments
			END AS payment_installments,
			CASE WHEN payment_value <= 0 THEN NULL
				 ELSE payment_value
			END AS payment_value,
			CASE WHEN (SUM(CASE WHEN payment_value <= 0 THEN 1 ELSE 0 END) 
					   OVER (PARTITION BY order_id)) > 0 THEN NULL
				 ELSE SUM(payment_value) OVER (PARTITION BY order_id) 
			END AS tot_payment_value
		FROM bronze.order_payments
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading silver.order_reviews
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.order_reviews';
		TRUNCATE TABLE silver.order_reviews;
		PRINT '>> Inserting Data Into: silver.order_reviews';
		INSERT INTO silver.order_reviews (
			review_id,
			order_id,
			review_score,
			review_comment_title,
			review_comment_message,
			review_creation_date,
			review_answer_timestamp
		)
		SELECT
			review_id,
			order_id,
			review_score,
			LOWER(TRIM(review_comment_title)),
			LOWER(TRIM(review_comment_message)),
			review_creation_date,
			review_answer_timestamp
		FROM bronze.order_reviews
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading silver.orders
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.orders';
		TRUNCATE TABLE silver.orders;
		PRINT '>> Inserting Data Into: silver.orders';
		INSERT INTO silver.orders (
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			order_delivered_customer_date,
			order_estimated_delivery_date
		)
		SELECT
			order_id,
			customer_id,
			order_status,
			order_purchase_timestamp,
			order_approved_at,
			order_delivered_carrier_date,
			CASE WHEN COALESCE(order_delivered_carrier_date, order_approved_at, order_purchase_timestamp) < order_delivered_customer_date THEN order_delivered_customer_date 
				 ELSE NULL
			END AS order_delivered_customer_date,
				CASE WHEN COALESCE(order_delivered_carrier_date, order_approved_at, order_purchase_timestamp) < order_estimated_delivery_date THEN order_estimated_delivery_date 
					 ELSE NULL
			END AS order_estimated_delivery_date
		FROM (
			SELECT 
				order_id,
				customer_id,
				order_status,
				order_purchase_timestamp,
				order_approved_at,
				CASE WHEN COALESCE(order_approved_at,order_purchase_timestamp) < order_delivered_carrier_date THEN order_delivered_carrier_date
					 ELSE NULL
				END AS order_delivered_carrier_date,
				order_delivered_customer_date,
				order_estimated_delivery_date
			FROM (
				SELECT
					order_id,
					customer_id,
					order_status,
					order_purchase_timestamp,
					CASE WHEN order_purchase_timestamp < order_approved_at AND 
							  order_status != 'created' THEN order_approved_at
						 ELSE NULL
					END AS order_approved_at,
					CASE WHEN order_status IN ('approved', 'created', 'invoiced', 'processing', 'unavailable', 'canceled') THEN NULL
						 ELSE order_delivered_carrier_date
					END AS order_delivered_carrier_date,
					CASE WHEN order_status IN ('approved', 'created', 'invoiced', 'processing', 'unavailable', 'canceled', 'shipped') THEN NULL
						 ELSE order_delivered_customer_date
					END AS order_delivered_customer_date,
					order_estimated_delivery_date
				FROM bronze.orders
			) t1
		) t2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.product_category_name_translation
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.product_category_name_translation';
		TRUNCATE TABLE silver.product_category_name_translation;
		PRINT '>> Inserting Data Into: silver.product_category_name_translation';
		INSERT INTO silver.product_category_name_translation (
			product_category_name,
			product_category_name_english
		)
		SELECT
			TRIM(REPLACE(product_category_name, '_', ' ')),
			TRIM(REPLACE(product_category_name_english, '_', ' '))
		FROM bronze.product_category_name_translation
		UNION 
		SELECT 'pc gamer', 'pc gamer'
		UNION 
		SELECT 'portateis cozinha e preparadores de alimentos', 'portable kitchen and food preparation appliances'
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.products
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.products';
		TRUNCATE TABLE silver.products;
		PRINT '>> Inserting Data Into: silver.products';
		INSERT INTO silver.products (
			product_id,
			product_category_name,
			product_name_length,
			product_description_length,
			product_photos_qty,
			product_weight_g,
			product_length_cm,
			product_height_cm,
			product_width_cm
		)
		SELECT
			a.product_id,
			b.product_category_name_english,
			a.product_name_lenght,
			a.product_description_lenght,
			a.product_photos_qty,
			a.product_weight_g,
			a.product_length_cm,
			a.product_height_cm,
			a.product_width_cm
		FROM bronze.products AS a
		LEFT JOIN silver.product_category_name_translation AS b
		ON TRIM(REPLACE(a.product_category_name, '_', ' ')) = b.product_category_name
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.sellers
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.sellers';
		TRUNCATE TABLE silver.sellers;
		PRINT '>> Inserting Data Into: silver.sellers';
		INSERT INTO silver.sellers (
			seller_id,
			seller_zip_code_prefix,
			seller_city,
			seller_state
		)
		SELECT
			seller_id,
			CASE 
				WHEN seller_zip_code_prefix = '37540' AND silver.clean_names_fn(seller_city) != 'santa rita do sapucai' THEN NULL
				WHEN seller_zip_code_prefix = '88075' AND silver.clean_names_fn(seller_city) != 'florianopolis' THEN NULL
				ELSE seller_zip_code_prefix
			END ,
			silver.clean_names_fn(seller_city),
			CASE 
				WHEN seller_zip_code_prefix = '44600' THEN 'BA'
				WHEN seller_zip_code_prefix IN ('21210', '22783') THEN 'RJ'
				WHEN seller_zip_code_prefix IN ('31160', '36010', '37795') THEN 'MG'
				WHEN seller_zip_code_prefix IN ('80240', '81020', '81560', '83020', '83321', '85960') THEN 'PR'
				WHEN seller_zip_code_prefix IN ('88301', '89052') THEN 'SC'
				WHEN seller_zip_code_prefix = '95076' THEN 'RS'
				ELSE seller_state
			END 
		FROM bronze.sellers
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
