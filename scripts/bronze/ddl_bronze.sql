/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.customers', 'U') IS NOT NULL
    DROP TABLE bronze.customers;
GO

CREATE TABLE bronze.customers (
	customer_id nvarchar(32),
	customer_unique_id nvarchar(32),
	customer_zip_code_prefix nvarchar(5),
	customer_city nvarchar(50),
	customer_state nvarchar(2)
);
GO

IF OBJECT_ID('bronze.geolocation', 'U') IS NOT NULL
    DROP TABLE bronze.geolocation;
GO

CREATE TABLE bronze.geolocation (
	geolocation_zip_code_prefix nvarchar(5),
	geolocation_lat float,
	geolocation_lng float,
	geolocation_city nvarchar(50),
	geolocation_state nvarchar(2)
);
GO

IF OBJECT_ID('bronze.order_items', 'U') IS NOT NULL
    DROP TABLE bronze.order_items;
GO

CREATE TABLE bronze.order_items (
    order_id nvarchar(32),
	order_item_id tinyint,
	product_id nvarchar(32),
	seller_id nvarchar(32),
	shipping_limit_date datetime2(7),
	price float,
	freight_value float
);
GO

IF OBJECT_ID('bronze.order_payments', 'U') IS NOT NULL
    DROP TABLE bronze.order_payments;
GO

CREATE TABLE bronze.order_payments (
	order_id nvarchar(32),
	payment_sequential tinyint,
	payment_type nvarchar(50),
	payment_installments tinyint,
	payment_value float
);
GO

IF OBJECT_ID('bronze.order_reviews', 'U') IS NOT NULL
    DROP TABLE bronze.order_reviews;
GO

CREATE TABLE bronze.order_reviews (
	review_id nvarchar(32),
	order_id nvarchar(32),
	review_score tinyint,
	review_comment_title nvarchar(50),
	review_comment_message nvarchar(max),
	review_creation_date datetime2(7),
	review_answer_timestamp datetime2(7)
);
GO

IF OBJECT_ID('bronze.orders', 'U') IS NOT NULL
    DROP TABLE bronze.orders;
GO

CREATE TABLE bronze.orders (
	order_id nvarchar(32),
	customer_id nvarchar(32),
	order_status nvarchar(50),
	order_purchase_timestamp datetime2(7),
	order_approved_at datetime2(7),
	order_delivered_carrier_date datetime2(7),
	order_delivered_customer_date datetime2(7),
	order_estimated_delivery_date datetime2(7)
);
GO

IF OBJECT_ID('bronze.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE bronze.product_category_name_translation;
GO

CREATE TABLE bronze.product_category_name_translation (
	product_category_name nvarchar(50),
	product_category_name_english nvarchar(50)
);
GO

IF OBJECT_ID('bronze.products', 'U') IS NOT NULL
    DROP TABLE bronze.products;
GO

CREATE TABLE bronze.products (
	product_id nvarchar(32),
	product_category_name nvarchar(50),
	product_name_lenght tinyint,
	product_description_lenght smallint,
	product_photos_qty tinyint,
	product_weight_g int,
	product_length_cm tinyint,
	product_height_cm tinyint,
	product_width_cm tinyint
);
GO

IF OBJECT_ID('bronze.sellers', 'U') IS NOT NULL
    DROP TABLE bronze.sellers;
GO

CREATE TABLE bronze.sellers (
	seller_id nvarchar(32),
	seller_zip_code_prefix nvarchar(5),
	seller_city nvarchar(50),
	seller_state nvarchar(2)
);
GO
