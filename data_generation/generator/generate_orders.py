import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import uuid

faker=Faker()

def generate_orders(customers_df,n=5000):
    new_count=int(n*0.7)
    update_count=int(n*0.2)
    duplicate_count=n-new_count-update_count
    orders=[]
    ## Generate new records
    for _ in range(new_count):
        customer=customers_df.sample(1).iloc[0]
        
        order_id=str(uuid.uuid4())
        amount=round(np.random.uniform(200,10000),2)
        now_ns = int(datetime.now().timestamp() * 1_000_000_000)
        
        orders.append({
            "ORDER_ID":order_id,
            "CUSTOMER_ID":customer["CUSTOMER_ID"],
            "ORDER_AMOUNT":amount,
            "CITY":customer["CITY"],
            "CREATED_AT":now_ns,
            "UPDATED_AT":now_ns,
            "SOURCE_SYSTEM":"OMS_V1"
        })
        
    orders_df= pd.DataFrame(orders)
    ## Generated updated records
    updates=orders_df.sample(update_count).copy()
    updates["ORDER_AMOUNT"]=updates["ORDER_AMOUNT"]*np.random.uniform(0.8,1.2)
    updates["UPDATED_AT"] = int(datetime.now().timestamp() * 1_000_000_000)
    ## Genetate the duplicate record
    duplicates=orders_df.sample(duplicate_count).copy()
    
    #Combine everything
    final_orders=pd.concat([orders_df,updates,duplicates],ignore_index=True)
    return final_orders 
    