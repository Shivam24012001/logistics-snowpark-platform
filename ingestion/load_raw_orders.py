from snowflake.snowpark import Session
import os
import uuid
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ---------------------------------------------------
# Connection Configuration
# ---------------------------------------------------
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

# ---------------------------------------------------
# Upload Files to Stage
# ---------------------------------------------------
def upload_files(session):
    local_path = r"data_generation/daily_batches/*.parquet"

    print("Uploading files from:", local_path)

    result = session.file.put(
        local_path,
        "@LOGISTICS_DB.BRONZE.RAW_STAGE",
        auto_compress=False,
        overwrite=True
    )

    print("Upload result:", result)


# ---------------------------------------------------
# Table Configuration (Pattern Only)
# ---------------------------------------------------
TABLE_CONFIG = {

    "RAW_CUSTOMERS": {
        "pattern": ".*customers.*"
    },

    "RAW_ORDERS": {
        "pattern": ".*orders.*"
    },

    "RAW_PAYMENTS": {
        "pattern": ".*payments.*"
    },

    "RAW_DELIVERIES": {
        "pattern": ".*deliveries.*"
    },

    "RAW_STATUS": {
        "pattern": ".*status.*"
    }
}





# ---------------------------------------------------
# Bronze COPY + Metadata + Audit
# ---------------------------------------------------
def copy_table(session, table_name, config):

    load_id = str(uuid.uuid4())
    load_start_time = datetime.now()

    copy_sql = f"""
    COPY INTO LOGISTICS_DB.BRONZE.{table_name}
    FROM @LOGISTICS_DB.BRONZE.RAW_STAGE
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    PATTERN = '{config["pattern"]}'
    ON_ERROR = 'CONTINUE'
    """

    print(f"\nLoading {table_name}...")

    try:
        result = session.sql(copy_sql).collect()
        load_end_time = datetime.now()

        # Update Metadata Columns
        metadata_sql = f"""
        UPDATE LOGISTICS_DB.BRONZE.{table_name}
        SET
            LOAD_TIMESTAMP = CURRENT_TIMESTAMP(),
            LOAD_DATE = CURRENT_DATE()
        WHERE LOAD_TIMESTAMP IS NULL;
        """
        session.sql(metadata_sql).collect()

        # Insert Audit Records
        for row in result:
            insert_audit_sql = f"""
            INSERT INTO LOGISTICS_DB.BRONZE.FILE_LOAD_AUDIT
            (
                LOAD_ID,
                TABLE_NAME,
                FILE_NAME,
                ROW_COUNT,
                LOAD_STATUS,
                LOAD_START_TIME,
                LOAD_END_TIME,
                ERROR_MESSAGE
            )
            VALUES
            (
                '{load_id}',
                '{table_name}',
                '{row["file"]}',
                {row["rows_loaded"]},
                '{row["status"]}',
                '{load_start_time}',
                '{load_end_time}',
                NULL
            )
            """
            session.sql(insert_audit_sql).collect()

        print(f"{table_name} loaded successfully.")

    except Exception as e:

        load_end_time = datetime.now()

        error_sql = f"""
        INSERT INTO LOGISTICS_DB.BRONZE.FILE_LOAD_AUDIT
        (
            LOAD_ID,
            TABLE_NAME,
            FILE_NAME,
            ROW_COUNT,
            LOAD_STATUS,
            LOAD_START_TIME,
            LOAD_END_TIME,
            ERROR_MESSAGE
        )
        VALUES
        (
            '{load_id}',
            '{table_name}',
            NULL,
            0,
            'FAILED',
            '{load_start_time}',
            '{load_end_time}',
            '{str(e).replace("'", "")}'
        )
        """
        session.sql(error_sql).collect()

        raise e


# ---------------------------------------------------
# Main Execution
# ---------------------------------------------------
if __name__ == "__main__":

    session = Session.builder.configs(connection_parameters).create()

    print("\nStarting Bronze Ingestion Pipeline...")

    upload_files(session)

    for table_name, config in TABLE_CONFIG.items():
        copy_table(session, table_name, config)

    print("\nBronze Load Completed Successfully.")

    session.close()