---timestamp validation
create or replace procedure dq_timestamp_validation(database_name VARCHAR, schema_name VARCHAR, table_name VARCHAR, col varchar)
returns variant
language sql
execute as caller
as
$$
declare

    timestamp_query varchar;
    timestamp_resultset resultset;
    latest_timestamp timestamp default to_timestamp(current_timestamp());
    cnt number DEFAULT 0;
    total_count variant;
    wrong_timestamps ARRAY DEFAULT [];
    log_array ARRAY DEFAULT [];

BEGIN

    timestamp_query := 'SELECT' || ' to_timestamp(' || col || ', ' || '''DD-MM-YYYY HH24:MI''' || ')' || ' AS transformed_timestamp_col FROM ' || database_name || '.' || schema_name || '.' || table_name || ' WHERE ' || 'to_timestamp(' || col || ', ' || '''DD-MM-YYYY HH24:MI''' || ')' || ' > ''' || latest_timestamp|| '''';
    
    timestamp_resultset := (EXECUTE IMMEDIATE :timestamp_query);
    
    FOR col_rec IN timestamp_resultset DO
        cnt := (cnt + 1);
        wrong_timestamps := ARRAY_APPEND(wrong_timestamps, col_rec.transformed_timestamp_col);
    END FOR;

    total_count := OBJECT_CONSTRUCT('current_timestamp', latest_timestamp,
                                    'column_name', col,
                                    'wrong_timestamp_cnt', cnt,
                                    'fault_timestamps', wrong_timestamps);

    log_array := ARRAY_APPEND(log_array, total_count);

    return log_array;

END;

$$;

call timestamp_validation('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'CLICKSTREAM_RAW', 'EVENT_TIMESTAMP');
call timestamp_validation('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'TRANSACTIONS_RAW', 'TRANSACTION_TIMESTAMP');