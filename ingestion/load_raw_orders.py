from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

load_dotenv()

connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

def load_orders(session):
    copy_sql = """
    COPY INTO LOGISTICS_DB.BRONZE.RAW_ORDERS
    FROM @LOGISTICS_DB.BRONZE.RAW_STAGE
    FILE_FORMAT=(TYPE=PARQUET)
    MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
    ON_ERROR='CONTINUE'
    """
    
    session.sql(copy_sql).collect()
    print("Data loaded successfully.")

if __name__ == "__main__":
    session = Session.builder.configs(connection_parameters).create()
    load_orders(session)
    session.close()