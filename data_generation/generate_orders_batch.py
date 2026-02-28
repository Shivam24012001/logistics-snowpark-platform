import pandas as pd
import numpy as np 
from faker import Faker
import uuid
from datetime import datetime ,timedelta
import os
import random 

fake =Faker()

cities=["Delhi","Mumbai","Banglore","Hyderabad","Chennai"]
payment_modes=["Netbanking","COD","Card","UPI"]
statues=["Placed","Delivered","Shipped","Cancelled"]

OUTPUT_DIR="data_generation/daily_batches"
os.makedirs(OUTPUT_DIR,exist_ok=True)

def generate_batch(batch_date,batch_number,num_records=5000):
    orders=[]
    
    for _ in range(num_records):
        order_id=str(uuid.uuid4()) ## Most common & safest and Random
        
        created=fake.date_time_between(start_date="-30d",end_date="now")
        updated=created+timedelta(hours=random.randint(1,48))
        
        orders.append({
            "ORDER_ID":order_id,
            "CUSTOMER_ID":str(uuid.uuid4()),
            "CITY":random.choice(cities),
            "ORDER_AMOUNT":round(np.random.uniform(100,10000),2),
            "ORDER_STATUS":random.choice(statues),
            "PAYMENT_MODE":random.choice(payment_modes),
            "CREATED_AT":created,
            "UPDATED_AT":updated
        })
        
    df=pd.DataFrame(orders) 
    file_name=f"orders_{batch_date}_batch{batch_number}.parquet"
    
    df.to_parquet(os.path.join(OUTPUT_DIR,file_name))
    
    print(f"Generated {file_name}")
    
if __name__=="__main__":
    today=datetime.today().strftime("%y_%m_%d")
    
    for i in range(1,4):
        generate_batch(today,i,5000)
    