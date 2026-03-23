import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import uuid
import random

fake =Faker()

def generate_customers(n=2000):
    new_count=int(n*0.7)
    update_count=int(n*0.2)
    duplicate_count=n-new_count-update_count
    customers=[]
    
    for _ in range(new_count):
        
        now_ns = int(datetime.now().timestamp() * 1_000_000_000)
        customers.append({
            "CUSTOMER_ID":str(uuid.uuid4()),
            "FIRST_NAME":fake.first_name(),
            "LAST_NAME":fake.last_name(),
            "EMAIL":fake.email(),
            "PHONE":fake.phone_number(),
            "CITY":fake.city(),
            "STATE":fake.state(),
            "CREATED_AT":now_ns,
            "UPDATED_AT":now_ns,
            "SOURCE_SYSTEM":"CRM_V2"
               
        })
    
    customer_df=pd.DataFrame(customers)
    
    ## generated the updated orders
    updates=customer_df.sample(update_count).copy()
    updates["EMAIL"]=updates['FIRST_NAME'].str.lower()+"@updated.com"
    updates['UPDATED_AT']=int(datetime.now().timestamp()*1_000_000_000)
    
    ##update the customer
    
    duplicate=customer_df.sample(duplicate_count).copy()
    ## combine
    final_customers=pd.concat(
        [customer_df,updates,duplicate],ignore_index=True
    )
    
    return final_customers
    