use database E_Commerce_Analytics;
use schema raw_ecommerce;

---file format for txt file with '~' delimiter
create or replace file format e_commerce_analytics_txt_format
type = 'csv'
field_delimiter = '~'
skip_header = 1;

---customers pipe
create or replace pipe customers_raw_pipe
auto_ingest = true
integration = 'AZURE_NOT_INT'
as
copy into E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.customers_raw from
(select 
$1 as user_id,
$2 as registration_date,
$3 as customer_segment,
$4 as first_name,
$5 as last_name,
$6 as email,
$7 as date_of_birth,
$8 as city,
$9 as state,
$10 as country
from @azure_ext_stage/customer/
(file_format => 'e_commerce_analytics_txt_format'));

ALTER PIPE customers_raw_pipe REFRESH;

select * from E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.CUSTOMERS_RAW;

---products pipe
create or replace pipe products_raw_pipe
auto_ingest = true
integration = 'AZURE_NOT_INT'
as
copy into E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.products_raw from
(select 
$1 as product_id,
$2 as product_name,
$3 as category,
$4 as price,
$5 as brand,
$6 as created_date,
$7 as is_active
from @azure_ext_stage/products/
(file_format => 'e_commerce_analytics_csv_format'));

ALTER PIPE products_raw_pipe REFRESH;

select * from E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.products_raw;

---promotions pipe
create or replace pipe promotions_raw_pipe
auto_ingest = true
integration = 'AZURE_NOT_INT'
as
copy into E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.promotions_raw from
(select 
$1 as promo_id,
$2 as promo_name,
$3 as start_date,
$4 as end_date,
$5 as discount_type,
$6 as discount_value,
$7 as target_audience
from @azure_ext_stage/promotions/
(file_format => 'e_commerce_analytics_csv_format'));

ALTER PIPE promotions_raw_pipe REFRESH;

select * from E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.promotions_raw;

---transactions pipe
create or replace pipe transactions_raw_pipe
auto_ingest = true
integration = 'AZURE_NOT_INT'
as
copy into E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.transactions_raw from
(select 
$1 as transaction_id,
$2 as user_id,
$3 as transaction_timestamp,
$4 as product_id,
$5 as quantity,
$6 as amount,
$7 as payment_method,
$8 as status,
$9 as session_id
from @azure_ext_stage/transactions/
(file_format => 'e_commerce_analytics_csv_format'));

ALTER PIPE transactions_raw_pipe REFRESH;

select * from E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.transactions_raw;