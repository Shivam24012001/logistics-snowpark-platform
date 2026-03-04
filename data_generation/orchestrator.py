from data_generation.generator.generate_customers import generate_customers
from data_generation.generator.generate_orders import generate_orders
from data_generation.generator.generate_payments import generate_payments
from data_generation.generator.generate_deliveries import generate_deliveries
from data_generation.generator.generate_status_events import generate_status_events

import os
from datetime import datetime

OUTPUT_DIR="data_generation/daily_batches"

os.makedirs(OUTPUT_DIR,exist_ok=True)

if __name__=='__main__':
    batch_id=datetime.now().strftime("%Y%m%d_%H%M%S")
    customers=generate_customers(2000)
    orders = generate_orders(customers, 5000) 
    payments=generate_payments(orders)
    deliveries=generate_deliveries(orders)
    status_events=generate_status_events(orders)
    
    customers.to_parquet(f"{OUTPUT_DIR}/customers_{batch_id}.parquet")
    orders.to_parquet(f"{OUTPUT_DIR}/orders_{batch_id}.parquet")
    payments.to_parquet(f"{OUTPUT_DIR}/payments_{batch_id}.parquet")
    deliveries.to_parquet(f"{OUTPUT_DIR}/deliveries_{batch_id}.parquet")
    status_events.to_parquet(f"{OUTPUT_DIR}/status_{batch_id}.parquet")
    
    print(f"Batch {batch_id} generated successfully.")