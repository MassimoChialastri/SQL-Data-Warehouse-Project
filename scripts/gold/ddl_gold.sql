/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'gold' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'gold' Tables
===============================================================================
*/

IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL
    DROP TABLE gold.dim_customer;
GO

CREATE TABLE gold.dim_customer (
	customer_key INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key
	customer_id nvarchar(32) NOT NULL,
	customer_unique_id nvarchar(32) NOT NULL,
	customer_zip_code_prefix nvarchar(5),
	customer_city nvarchar(50),
	customer_state nvarchar(2),
	customer_latitude float,
	customer_longitude float,
	customer_start_date date,
	customer_end_date date
);
GO

IF OBJECT_ID('gold.dim_product', 'U') IS NOT NULL
    DROP TABLE gold.dim_product;
GO

CREATE TABLE gold.dim_product (
	product_key INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key
	product_id nvarchar(32),
	product_category_name nvarchar(50),
	product_name_length tinyint,
	product_description_length smallint,
	product_photos_qty tinyint,
	product_weight_g int,
	product_length_cm tinyint,
	product_height_cm tinyint,
	product_width_cm tinyint
);
GO

IF OBJECT_ID('gold.dim_seller', 'U') IS NOT NULL
    DROP TABLE gold.dim_seller;
GO

CREATE TABLE gold.dim_seller (
	seller_key INT IDENTITY(1,1) PRIMARY KEY,
	seller_id nvarchar(32),
	seller_zip_code_prefix nvarchar(5),
	seller_city nvarchar(50),
	seller_state nvarchar(2),
	seller_latitude float,
	seller_longitude float
);
GO

IF OBJECT_ID('gold.fact_orders', 'U') IS NOT NULL
    DROP TABLE gold.fact_orders;
GO

CREATE TABLE gold.fact_orders (
	order_key INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key
    order_id nvarchar(32),
	customer_key int,
	order_items_number tinyint,
	order_payment float,
	payment_methods_number tinyint,
	order_status nvarchar(50),
	order_purchase_timestamp datetime2(7),
	order_approved_at datetime2(7),
	order_delivered_carrier_date datetime2(7),
	order_delivered_customer_date datetime2(7),
	order_estimated_delivery_date datetime2(7)
);
GO

IF OBJECT_ID('gold.fact_order_reviews', 'U') IS NOT NULL
    DROP TABLE gold.fact_order_reviews;
GO

CREATE TABLE gold.fact_order_reviews (
	review_key INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key
	review_id nvarchar(32),
	order_key int,
	review_score tinyint,
	review_comment_title nvarchar(50),
	review_comment_message nvarchar(max),
	review_survey_creation_date datetime2(7),
	review_answer_timestamp datetime2(7)
);
GO

IF OBJECT_ID('gold.fact_order_items', 'U') IS NOT NULL
    DROP TABLE gold.fact_order_items;
GO

CREATE TABLE gold.fact_order_items (
	order_items_key INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key
	order_key int,
	order_item_number tinyint,
	product_key int,
	seller_key int,
	price float,
	freight_value float,
	shipping_limit_date datetime2(7)
);
GO

IF OBJECT_ID('gold.fact_order_payments', 'U') IS NOT NULL
    DROP TABLE gold.fact_order_payments;
GO

CREATE TABLE gold.fact_order_payments (
	order_payment_key INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key
	order_key int,
	payment_sequential tinyint,
	payment_type nvarchar(50),
	payment_installments tinyint,
	payment_value float
);
GO

IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_date (
    date DATE NOT NULL PRIMARY KEY,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month_number INT NOT NULL,
    month VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_name VARCHAR(20) NOT NULL
);