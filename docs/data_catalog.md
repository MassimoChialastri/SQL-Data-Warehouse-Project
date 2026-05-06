# Data Catalog for Gold Layer

## Overview
The Gold Layer is the business-level data representation, structured to support analytical and reporting use cases. It consists of **dimension tables** and **fact tables** for specific business metrics.

---

### 1. **dim_customer**
- **Purpose:** Stores customer details enriched with geographic and temporal data, used to identify and segment customers across orders.
- **Columns:**

| Column Name              | Data Type     | Description                                                                                   |
|--------------------------|---------------|-----------------------------------------------------------------------------------------------|
| customer_key             | INT           | Surrogate key uniquely identifying each customer record in the dimension table.               |
| customer_id              | NVARCHAR(32)  | Unique numerical identifier assigned to each customer.                                        |
| customer_unique_id       | NVARCHAR(32)  | Alphanumeric unique identifier for the customer, used for deduplication and cross-referencing.|
| customer_zip_code_prefix | NVARCHAR(5)   | The ZIP code prefix associated with the customer's address, used for geographic grouping.     |
| customer_city            | NVARCHAR(50)  | The city of residence of the customer.                                                        |
| customer_state           | NVARCHAR(2)   | The state of residence of the customer (e.g., 'SP', 'RJ').                                    |
| customer_latitude        | FLOAT         | Geographic latitude coordinate of the customer's location.                                    |
| customer_longitude       | FLOAT         | Geographic longitude coordinate of the customer's location.                                   |
| customer_start_date      | DATE          | The date from which the customer record becomes valid (SCD type 2 support).                   |
| customer_end_date        | DATE          | The date until which the customer record is valid; NULL if currently active.                  |

---

### 2. **dim_seller**
- **Purpose:** Provides information about sellers, including their geographic location, used to analyze seller performance and distribution.
- **Columns:**

| Column Name              | Data Type     | Description                                                                                   |
|--------------------------|---------------|-----------------------------------------------------------------------------------------------|
| seller_key               | INT           | Surrogate key uniquely identifying each seller record in the dimension table.                 |
| seller_id                | NVARCHAR(32)  | Unique identifier assigned to the seller for internal tracking and referencing.               |
| seller_zip_code_prefix   | NVARCHAR(5)   | The ZIP code prefix of the seller's location, used for geographic grouping.                   |
| seller_city              | NVARCHAR(50)  | The city where the seller operates.                                                           |
| seller_state             | NVARCHAR(2)   | The state where the seller operates (e.g., 'SP', 'MG').                                       |
| seller_latitude          | FLOAT         | Geographic latitude coordinate of the seller's location.                                      |
| seller_longitude         | FLOAT         | Geographic longitude coordinate of the seller's location.                                     |

---

### 3. **dim_product**
- **Purpose:** Provides information about the products and their physical and descriptive attributes.
- **Columns:**

| Column Name                  | Data Type     | Description                                                                                   |
|------------------------------|---------------|-----------------------------------------------------------------------------------------------|
| product_key                  | INT           | Surrogate key uniquely identifying each product record in the product dimension table.        |
| product_id                   | NVARCHAR(32)  | A unique identifier assigned to the product for internal tracking and referencing.            |
| product_category_name        | NVARCHAR(50)  | The name of the category the product belongs to (e.g., 'electronics', 'furniture').           |
| product_name_length          | TINYINT       | The character length of the product name, used for data quality analysis.                     |
| product_description_length   | SMALLINT      | The character length of the product description, used for data quality analysis.              |
| product_photos_quantity      | TINYINT       | The number of photos associated with the product listing.                                     |
| product_weight_grams         | INT           | The weight of the product in grams, used for logistics and freight calculations.              |
| product_length_cm            | TINYINT       | The length of the product in centimeters.                                                     |
| product_height_cm            | TINYINT       | The height of the product in centimeters.                                                     |
| product_width_cm             | TINYINT       | The width of the product in centimeters.                                                      |

---

### 4. **dim_date**
- **Purpose:** Provides a standard date dimension to support time-based analysis and reporting across all fact tables.
- **Columns:**

| Column Name   | Data Type     | Description                                                                                   |
|---------------|---------------|-----------------------------------------------------------------------------------------------|
| date          | DATE          | Primary key. The calendar date in YYYY-MM-DD format.                                          |
| year          | INT           | The four-digit calendar year (e.g., 2024).                                                    |
| quarter       | INT           | The calendar quarter of the year (1–4).                                                       |
| month_number  | INT           | The numeric month of the year (1–12).                                                         |
| month         | VARCHAR(20)   | The full name of the month (e.g., 'January', 'February').                                     |
| day           | INT           | The day of the month (1–31).                                                                  |
| day_name      | VARCHAR(20)   | The name of the day of the week (e.g., 'Monday', 'Tuesday').                                  |

---

### 5. **fact_orders**
- **Purpose:** Stores transactional order data, capturing the full lifecycle of each order from purchase to delivery. Central fact table linked to customers, dates, payments, items, and reviews.
- **Columns:**

| Column Name                    | Data Type     | Description                                                                                   |
|--------------------------------|---------------|-----------------------------------------------------------------------------------------------|
| order_key                      | INT           | Surrogate key uniquely identifying each order record. Primary key of the fact table.          |
| order_id                       | NVARCHAR(32)  | Unique alphanumeric identifier for each order (e.g., 'e481f51cbdc54678b7cc49136f2d6af7').     |
| customer_key                   | INT           | Surrogate key linking the order to the customer dimension table.                              |
| order_items_number             | TINYINT       | The total number of items included in the order.                                              |
| order_payment                  | FLOAT         | The total monetary value paid for the order.                                                  |
| payment_methods_number         | TINYINT       | The number of distinct payment methods used for the order.                                    |
| order_status                   | NVARCHAR(50)  | The current status of the order (e.g., 'delivered', 'shipped', 'canceled').                   |
| order_purchase_timestamp       | DATETIME2(7)  | Foreign key to dim_date. The date when the order was placed by the customer.                  |
| order_approved_at              | DATETIME2(7)  | The date and time when the order payment was approved.                                        |
| order_delivered_carrier_date   | DATETIME2(7)  | The date when the order was handed over to the logistics carrier.                             |
| order_delivered_customer_date  | DATETIME2(7)  | The date when the order was actually delivered to the customer.                               |
| order_estimated_delivery_date  | DATETIME2(7)  | Foreign key to dim_date. The estimated delivery date provided to the customer at purchase.    |

---

### 6. **fact_order_items**
- **Purpose:** Stores line-item level data for each order, capturing details about each product sold, the associated seller, pricing, and shipping constraints.
- **Columns:**

| Column Name         | Data Type     | Description                                                                                   |
|---------------------|---------------|-----------------------------------------------------------------------------------------------|
| order_items_key     | INT           | Surrogate key uniquely identifying each order item record. Primary key of the fact table.     |
| order_key           | INT           | Surrogate key linking the order item to the fact_orders table.                                |
| order_item_number   | TINYINT       | Sequential number identifying the item within the order (e.g., 1st, 2nd item).                |
| product_key         | INT           | Surrogate key linking the item to the product dimension table.                                |
| seller_key          | INT           | Surrogate key linking the item to the seller dimension table.                                 |
| price               | FLOAT         | The price of the product for this order item, in monetary units.                              |
| freight_value       | FLOAT         | The freight cost charged for shipping this specific order item.                               |
| shopping_limit_date | DATETIME2(7)  | The deadline by which the seller must dispatch the order item.                                |

---

### 7. **fact_order_payments**
- **Purpose:** Stores payment transaction details for each order, supporting analysis of payment methods, installments, and payment values.
- **Columns:**

| Column Name          | Data Type     | Description                                                                                   |
|----------------------|---------------|-----------------------------------------------------------------------------------------------|
| order_payment_key    | INT           | Surrogate key uniquely identifying each payment record. Primary key of the fact table.        |
| order_key            | INT           | Surrogate key linking the payment to the fact_orders table.                                   |
| payment_sequential   | TINYINT       | Sequential number indicating the order of payments when multiple methods are used.            |
| payment_type         | NVARCHAR(50)  | The method of payment used (e.g., 'credit_card', 'boleto', 'voucher', 'debit_card').          |
| payment_installments | TINYINT       | The number of installments chosen by the customer for the payment.                            |
| payment_value        | FLOAT         | The monetary value of the payment transaction, in whole currency units.                       |

---

### 8. **fact_order_reviews**
- **Purpose:** Stores customer review data associated with orders, enabling satisfaction and sentiment analysis.
- **Columns:**

| Column Name              | Data Type     | Description                                                                                   |
|--------------------------|---------------|-----------------------------------------------------------------------------------------------|
| review_key               | INT           | Surrogate key uniquely identifying each review record. Primary key of the fact table.         |
| review_id                | NVARCHAR(32)  | Unique alphanumeric identifier for the review.                                                |
| order_key                | INT           | Surrogate key linking the review to the fact_orders table.                                    |
| review_score             | TINYINT       | Customer satisfaction score on a scale from 1 (worst) to 5 (best).                            |
| review_comment_title     | NVARCHAR(50)  | The title or headline of the review comment left by the customer.                             |
| review_comment_message   | NVARCHAR(MAX) | The full text message of the review left by the customer.                                     |
| review_survey_creation   | DATETIME2(7)  | The date and time when the review survey was sent to the customer.                            |
| review_answer_timestamp  | DATETIME2(7)  | The date and time when the customer submitted the review response.                            |
