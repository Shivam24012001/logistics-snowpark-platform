import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import uuid
import random

fake =Faker()

def generate_customers(n=2000):
    customer=[]
    
    for _ in range(n):
        now_ns = int(datetime.now().timestamp() * 1_000_000_000)
        customer.append({
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
    
    return pd.DataFrame(customer)