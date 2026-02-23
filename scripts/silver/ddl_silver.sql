/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

IF OBJECT_ID('silver.customers', 'U') IS NOT NULL
    DROP TABLE silver.customers;
GO

CREATE TABLE silver.customers (
	customer_id nvarchar(32),
	customer_unique_id nvarchar(32),
	customer_zip_code_prefix nvarchar(5),
	customer_city nvarchar(50),
	customer_state nvarchar(2),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.geolocation', 'U') IS NOT NULL
    DROP TABLE silver.geolocation;
GO

CREATE TABLE silver.geolocation (
	geolocation_zip_code_prefix nvarchar(5),
	geolocation_lat float,
	geolocation_lng float,
	geolocation_city nvarchar(50),
	geolocation_state nvarchar(2),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.order_items', 'U') IS NOT NULL
    DROP TABLE silver.order_items;
GO

CREATE TABLE silver.order_items (
    order_id nvarchar(32),
	order_item_id tinyint,
	product_id nvarchar(32),
	seller_id nvarchar(32),
	shipping_limit_date datetime2(7),
	price float,
	freight_value float,
	total_price float,
	total_freight_value float,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.order_payments', 'U') IS NOT NULL
    DROP TABLE silver.order_payments;
GO

CREATE TABLE silver.order_payments (
	order_id nvarchar(32),
	payment_sequential tinyint,
	payment_type nvarchar(50),
	payment_installments tinyint,
	payment_value float,
	tot_payment_value float,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.order_reviews', 'U') IS NOT NULL
    DROP TABLE silver.order_reviews;
GO

CREATE TABLE silver.order_reviews (
	review_id nvarchar(32),
	order_id nvarchar(32),
	review_score tinyint,
	review_comment_title nvarchar(50),
	review_comment_message nvarchar(max),
	review_creation_date datetime2(7),
	review_answer_timestamp datetime2(7),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.orders', 'U') IS NOT NULL
    DROP TABLE silver.orders;
GO

CREATE TABLE silver.orders (
	order_id nvarchar(32),
	customer_id nvarchar(32),
	order_status nvarchar(50),
	order_purchase_timestamp datetime2(7),
	order_approved_at datetime2(7),
	order_delivered_carrier_date datetime2(7),
	order_delivered_customer_date datetime2(7),
	order_estimated_delivery_date datetime2(7),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE silver.product_category_name_translation;
GO

CREATE TABLE silver.product_category_name_translation (
	product_category_name nvarchar(50),
	product_category_name_english nvarchar(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;
GO

CREATE TABLE silver.products (
	product_id nvarchar(32),
	product_category_name nvarchar(50),
	product_name_length tinyint,
	product_description_length smallint,
	product_photos_qty tinyint,
	product_weight_g int,
	product_length_cm tinyint,
	product_height_cm tinyint,
	product_width_cm tinyint,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.sellers', 'U') IS NOT NULL
    DROP TABLE silver.sellers;
GO

CREATE TABLE silver.sellers (
	seller_id nvarchar(32),
	seller_zip_code_prefix nvarchar(5),
	seller_city nvarchar(50),
	seller_state nvarchar(2),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
