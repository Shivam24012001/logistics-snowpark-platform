import pandas as pd
import numpy as np
from Faker import faker
from datetime import datetime, timedelta
import uuid

def generate_orders(customer_df,n=5000):
    orders=[]
    
    for _ in range(n):
        customer=customer_df.sample(1).iloc[0]
        order_id=str(uuid.uuid4())
        amount=round(np.random.uniform(200,10000),2)
        
        orders.apppen({
            "ORDER_ID":order_id,
            "CUSTOMER_ID":customer["CUSTOMER_ID"],
            "ORDER_AMOUNT":amount,
            "CITY":customer["CITY"],
            "CREATED_AT":datetime.now(),
            "UPDATED_AT":datetime.now(),
            "SOURCE_SYSTEM":"OMS_V1"
        })
        
    return pd.DataFrame(orders)