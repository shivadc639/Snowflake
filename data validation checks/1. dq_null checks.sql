use database E_Commerce_Analytics;
use schema raw_ecommerce;

CREATE OR REPLACE PROCEDURE dq_checks(database_name VARCHAR, schema_name VARCHAR, table_name VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    get_col_names_query VARCHAR;
    col_name_resultset RESULTSET;
    col_name_count_query VARCHAR;
    col_count NUMBER DEFAULT 0;
    count_resultset RESULTSET;
    null_count NUMBER;
    total_table_count NUMBER;
    percentage_null NUMBER;
    log_array ARRAY DEFAULT [];
    col_record VARIANT;
    
BEGIN
 
    get_col_names_query := 'SELECT COLUMN_NAME ' ||
                           'FROM ' || database_name || '.INFORMATION_SCHEMA.COLUMNS ' ||
                           'WHERE TABLE_CATALOG = ''' || database_name || ''' ' ||
                           'AND TABLE_SCHEMA = ''' || schema_name || ''' ' ||
                           'AND TABLE_NAME = ''' || table_name || ''' ';
    
    col_name_resultset := (EXECUTE IMMEDIATE :get_col_names_query);
    

    col_name_count_query := 'SELECT COUNT(*) AS CNT ' ||
                            'FROM ' || database_name || '.INFORMATION_SCHEMA.COLUMNS ' ||
                            'WHERE TABLE_CATALOG = ''' || database_name || ''' ' ||
                            'AND TABLE_SCHEMA = ''' || schema_name || ''' ' ||
                            'AND TABLE_NAME = ''' || table_name || '''';
    
    count_resultset := (EXECUTE IMMEDIATE :col_name_count_query);
    
    LET cur CURSOR FOR count_resultset;
    OPEN cur;
    FETCH cur INTO col_count;
    CLOSE cur;
    
    LET total_count_query VARCHAR := 'SELECT COUNT(*) FROM ' || database_name || '.' || schema_name || '.' || table_name;
    LET total_resultset RESULTSET := (EXECUTE IMMEDIATE :total_count_query);
    LET total_cur CURSOR FOR total_resultset;
    
    OPEN total_cur;
    FETCH total_cur INTO total_table_count;
    CLOSE total_cur;
    
    IF (col_count > 0) THEN
        LET col_cur CURSOR FOR col_name_resultset;
        
        FOR col_rec IN col_cur DO
        
            LET null_query VARCHAR := 'SELECT COUNT(*) FROM ' || database_name || '.' || schema_name || '.' || table_name || 
                                     ' WHERE "' || col_rec.COLUMN_NAME || '" IS NULL';
            LET null_resultset RESULTSET := (EXECUTE IMMEDIATE :null_query);
            LET null_cur CURSOR FOR null_resultset;
            
            OPEN null_cur;
            FETCH null_cur INTO null_count;
            CLOSE null_cur;
            
            percentage_null := (null_count / total_table_count) * 100;
            
            col_record := OBJECT_CONSTRUCT(
                                            'column_name', col_rec.COLUMN_NAME,
                                            'null_count', null_count,
                                            'total_count', total_table_count,
                                            'null_percentage', percentage_null
            );

            log_array := ARRAY_APPEND(log_array, col_record);
            
        END FOR;
    END IF;

    RETURN log_array;
    
END;
$$;

CALL dq_checks('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'CLICKSTREAM_RAW');
CALL dq_checks('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'CUSTOMERS_RAW');
CALL dq_checks('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'PRODUCTS_RAW');
CALL dq_checks('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'PROMOTIONS_RAW');
CALL dq_checks('E_COMMERCE_ANALYTICS', 'RAW_ECOMMERCE', 'TRANSACTIONS_RAW');
