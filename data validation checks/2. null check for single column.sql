create or replace procedure empty_string_val(database_name VARCHAR, schema_name VARCHAR, table_name VARCHAR, col varchar)
returns variant
language sql
execute as caller
as
$$

DECLARE

        col_sql varchar;
        col_resultset resultset;
        col_null_count number;
        col_record variant;

BEGIN

        col_sql := 'SELECT COUNT(*) FROM ' || database_name || '.' || schema_name || '.' || table_name || ' WHERE ' || col || ' IS NULL';

        col_resultset := (EXECUTE IMMEDIATE :col_sql);

        LET cur CURSOR FOR col_resultset;
        OPEN cur;
        FETCH cur INTO col_null_count;
        CLOSE cur;

        IF (col_null_count > 0) THEN
        col_record := OBJECT_CONSTRUCT('column_name', col,
                                        'null_count', col_null_count);
                                        
        END IF;

        RETURN col_record;

END;

$$;

call empty_string_val('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'CLICKSTREAM_RAW', 'PRODUCT_ID');