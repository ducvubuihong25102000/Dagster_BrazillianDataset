DROP TABLE IF EXISTS product_category_name_translation;
CREATE TABLE product_category_name_translation (
product_category_name varchar(64),
product_category_name_english varchar(64),
PRIMARY KEY (product_category_name)
);
DROP TABLE IF EXISTS olist_products_dataset;
CREATE TABLE olist_products_dataset (
product_id varchar(32),
product_category_name varchar(64),
product_name_lenght int4,
product_description_lenght int4,
product_photos_qty int4,
product_weight_g int4,
product_length_cm int4,
product_height_cm int4,
product_width_cm int4,
PRIMARY KEY (product_id)
);
DROP TABLE IF EXISTS olist_orders_dataset;
CREATE TABLE olist_orders_dataset (
order_id varchar(32),
customer_id varchar(32),
order_status varchar(16),
order_purchase_timestamp varchar(32),
order_approved_at varchar(32),
order_delivered_carrier_date varchar(32),
