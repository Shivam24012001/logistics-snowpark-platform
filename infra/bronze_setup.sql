-- create Stage

create or replace stage logistics_db.bronze.raw_stage;

-- Create Raw table 

create or replace table logistics_db.bronze.raw_orders(
    ORDER_ID STRING,
    CUSTOMER_ID STRING,
    CITY STRING,
    ORDER_AMOUNT NUMBER (10,2),
    ORDER_STATUS STRING,
    PAYMENT_MODE STRING,
    CREATED_AT TIMESTAMP,
    UPDATED_AT TIMESTAMP,
    LOAD_TIMESTAMP TIMESTAMP  DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING,
    LOAD_ID STRING,
    LOAD_DATE DATE DEFAULT CURRENT_DATE()

)