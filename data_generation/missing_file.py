import os
import glob
from dotenv import load_dotenv
from snowflake.snowpark import Session

# -----------------------------------------
# Load environment variables
# -----------------------------------------

load_dotenv()

STAGE_NAME = "@LOGISTICS_DB.BRONZE.RAW_STAGE"

# -----------------------------------------
# Snowflake Connection
# -----------------------------------------

connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

session = Session.builder.configs(connection_parameters).create()

print("Connected to Snowflake")

# -----------------------------------------
# Get files already in stage
# -----------------------------------------

stage_files = session.sql(f"LIST {STAGE_NAME}").collect()

stage_file_names = [
    row['name'].split('/')[-1] for row in stage_files
]

print(f"Files already in stage: {len(stage_file_names)}")

# -----------------------------------------
# Get local parquet files
# -----------------------------------------

local_files = glob.glob("data_generation/batches/**/*.parquet", recursive=True)

print(f"Local files found: {len(local_files)}")

# -----------------------------------------
# Upload only missing files
# -----------------------------------------

uploaded_count = 0

for file in local_files:

    filename = os.path.basename(file)

    if filename not in stage_file_names:

        session.file.put(
            file,
            STAGE_NAME,
            auto_compress=False
        )

        print(f"Uploaded missing file: {filename}")

        uploaded_count += 1


print(f"\nTotal missing files uploaded: {uploaded_count}")

# -----------------------------------------
# Close session
# -----------------------------------------

session.close()

print("Upload process completed.")