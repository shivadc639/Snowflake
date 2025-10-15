use database E_Commerce_Analytics;
use schema raw_ecommerce;

create or replace table clicstream_raw(
event_timestamp varchar,
user_id varchar,
session_id varchar,
page_url varchar,
event_type varchar,
product_id varchar,
referrer_url varchar,
user_agent varchar,
ip_address varchar
);

create or replace table transactions_raw(
transaction_id varchar,
user_id varchar,
transaction_timestamp varchar,
product_id varchar,
quantity varchar,
amount varchar,
payment_method varchar,
status varchar,
session_id varchar
);

create or replace table customers_raw(
user_id varchar,
registration_date varchar,
customer_segment varchar,
first_name varchar,
last_name varchar,
email varchar,
date_of_birth varchar,
city varchar,
state varchar,
country varchar
);

create or replace table products_raw ( 
product_id varchar,
prodcut_name varchar,
category varchar,
price varchar,
brand varchar,
created_date varchar,
is_active varchar);

create or replace table promotions_raw (
promo_id varchar,
promo_name varchar,
start_date varchar,
end_date varchar,
discount_type varchar,
discount_value varchar,
target_audience varchar
);

---STG tables
use database E_Commerce_Analytics;
use schema staging_ecommerce;

create or replace table clicstream_stg(
event_timestamp timestamp,
user_id number,
session_id varchar,
page_url varchar,
event_type varchar,
product_id varchar,
referrer_url varchar,
user_agent varchar,
ip_address number
);

create or replace table transactions_stg(
transaction_id varchar,
user_id number,
transaction_timestamp timestamp,
product_id varchar,
quantity number,
amount number,
payment_method varchar,
status varchar,
session_id varchar
);

create or replace table customers_stg(
user_id number,
registration_date date,
customer_segment varchar,
first_name varchar,
last_name varchar,
email varchar,
date_of_birth date,
city varchar,
state varchar,
country varchar
);

create or replace table products_stg ( 
product_id varchar,
prodcut_name varchar,
category varchar,
price number,
brand varchar,
created_date date,
is_active varchar);

create or replace table promotions_stg (
promo_id varchar,
promo_name varchar,
start_date date,
end_date date,
discount_type varchar,
discount_value number,
target_audience varchar
);