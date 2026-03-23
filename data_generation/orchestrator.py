import os
import time
from datetime import datetime
from dotenv import load_dotenv
from snowflake.snowpark import Session

from data_generation.generator.generate_customers import generate_customers
from data_generation.generator.generate_orders import generate_orders
from data_generation.generator.generate_payments import generate_payments
from data_generation.generator.generate_deliveries import generate_deliveries
from data_generation.generator.generate_status_events import generate_status_events


load_dotenv()

STAGE_NAME = "@LOGISTICS_DB.BRONZE.RAW_STAGE"


# --------------------------------------------------
# Snowflake Session
# --------------------------------------------------

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


# --------------------------------------------------
# Generate Batch
# --------------------------------------------------

def generate_batch():

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_path = datetime.now().strftime("%Y/%m/%d")

    output_dir = f"data_generation/batches/orders/{date_path}"
    os.makedirs(output_dir, exist_ok=True)

    customers = generate_customers(2000)
    orders = generate_orders(customers, 5000)
    payments = generate_payments(orders)
    deliveries = generate_deliveries(orders)
    status_events = generate_status_events(orders)

    customers_file = f"{output_dir}/customers_{batch_id}.parquet"
    orders_file = f"{output_dir}/orders_{batch_id}.parquet"
    payments_file = f"{output_dir}/payments_{batch_id}.parquet"
    deliveries_file = f"{output_dir}/deliveries_{batch_id}.parquet"
    status_file = f"{output_dir}/status_{batch_id}.parquet"

    customers.to_parquet(customers_file)
    orders.to_parquet(orders_file)
    payments.to_parquet(payments_file)
    deliveries.to_parquet(deliveries_file)
    status_events.to_parquet(status_file)

    print(f"Batch {batch_id} generated successfully.")

    # --------------------------------------------------
    # Upload files to Snowflake stage
    # --------------------------------------------------

    session.file.put(
        f"{output_dir}/*.parquet",
        STAGE_NAME,
        auto_compress=False,
        overwrite=True
    )

    print("Files uploaded to Snowflake stage.")


# --------------------------------------------------
# Continuous Pipeline
# --------------------------------------------------

if __name__ == "__main__":

    while True:

        generate_batch()

        print("Waiting 10 minutes for next batch...")

        time.sleep(600)