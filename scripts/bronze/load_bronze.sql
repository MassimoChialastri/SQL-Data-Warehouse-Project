/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.customers';
		TRUNCATE TABLE bronze.customers;
		PRINT '>> Inserting Data Into: bronze.customers';
		BULK INSERT bronze.customers
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\customers.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.geolocation';
		TRUNCATE TABLE bronze.geolocation;
		PRINT '>> Inserting Data Into: bronze.geolocation';
		BULK INSERT bronze.geolocation
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\geolocation.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.order_items';
		TRUNCATE TABLE bronze.order_items;
		PRINT '>> Inserting Data Into: bronze.order_items';
		BULK INSERT bronze.order_items
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\order_items.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.order_payments';
		TRUNCATE TABLE bronze.order_payments;
		PRINT '>> Inserting Data Into: bronze.order_payments';
		BULK INSERT bronze.order_payments
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\order_payments.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
        

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.order_reviews';
		TRUNCATE TABLE bronze.order_reviews;
		PRINT '>> Inserting Data Into: bronze.order_reviews';
		BULK INSERT bronze.order_reviews
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\order_reviews.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '\n',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.orders';
		TRUNCATE TABLE bronze.orders;
		PRINT '>> Inserting Data Into: bronze.orders';
		BULK INSERT bronze.orders
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\orders.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.product_category_name_translation';
		TRUNCATE TABLE bronze.product_category_name_translation;
		PRINT '>> Inserting Data Into: bronze.product_category_name_translation';
		BULK INSERT bronze.product_category_name_translation
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\product_category_name_translation.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.products';
		TRUNCATE TABLE bronze.products;
		PRINT '>> Inserting Data Into: bronze.products';
		BULK INSERT bronze.products
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\products.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.sellers';
		TRUNCATE TABLE bronze.sellers;
		PRINT '>> Inserting Data Into: bronze.sellers';
		BULK INSERT bronze.sellers
		FROM 'C:\SQL-Data-Warehouse-Project\datasets\sellers.csv'
		WITH (
			FIRSTROW = 2,
			ROWTERMINATOR = '0x0a',
			FORMAT = 'CSV',
			CODEPAGE = '65001',
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
