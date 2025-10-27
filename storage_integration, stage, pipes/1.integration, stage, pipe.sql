create or replace storage integration azure_int
type = EXTERNAL_STAGE
storage_provider = 'Azure'
enabled = TRUE
azure_tenant_id = '___'
storage_allowed_locations = ('azure://snowflakepractise.blob.core.windows.net/snowflakeoct2025/');

show storage integrations;

desc integration azure_int;

select SYSTEM$VALIDATE_STORAGE_INTEGRATION( 'AZURE_INT', 'azure://snowflakepractise.blob.core.windows.net/snowflakeoct2025/', 'clickstream_20240115_1402.csv', 'all' );

show storage integrations;

create or replace notification integration azure_not_int
enabled = true
type = queue
notification_provider = azure_storage_queue
azure_storage_queue_primary_uri = 'https://snowflakepractise.queue.core.windows.net/queue1'
azure_tenant_id = '___';

show notification integrations;

desc notification integration azure_not_int;

create or replace file format e_commerce_analytics_csv_format
type = csv
field_delimiter = ','
skip_header = 1;

create or replace stage azure_ext_stage
url = 'azure://snowflakepractise.blob.core.windows.net/snowflakeoct2025/ecommerce/'
storage_integration = azure_int;

desc stage azure_ext_stage;

list @AZURE_EXT_STAGE ;

create or replace pipe clickstream_raw_pipe
auto_ingest = true
integration = 'AZURE_NOT_INT'
as
copy into E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.CLICKSTREAM_RAW from
(select 
$1 as event_timestamp,
$2 as user_id,
$3 as session_id,
$4 as page_url,
$5 as event_type,
$6 as product_id,
$7 as referrer_url,
$8 as user_agent,
$9 as ip_address
from @azure_ext_stage/click-stream/
(file_format => 'e_commerce_analytics_csv_format'));

ALTER PIPE clickstream_raw_pipe REFRESH;

select * from E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.CLICKSTREAM_RAW;

select system$pipe_status('E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.CLICKSTREAM_RAW_PIPE');

select * from table (information_schema.copy_history(table_name => 'E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.CLICKSTREAM_RAW', start_time => DATEADD(HOUR, -2, CURRENT_TIMESTAMP())));

select * from table (information_schema.pipe_usage_history(pipe_name => 'E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.CLICKSTREAM_RAW_PIPE'));

select * from table(validate_pipe_load(pipe_name => 'E_COMMERCE_ANALYTICS.RAW_ECOMMERCE.CLICKSTREAM_RAW_PIPE', start_time => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())));