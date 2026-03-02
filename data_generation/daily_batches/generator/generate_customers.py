import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import uuid
import random

fake =Faker()

def generate_customer(n=2000):
    customer=[]
    
    for _ in range(n):
        customer.append({
            "CUSTOMER_ID":str(uuid.uuid4()),
            "FIRST_NAME":fake.first_name(),
            "LAST_NAME":fake.last_name(),
            "EMAIL":fake.email(),
            "PHONE":fake.phone(),
            "CITY":fake.city(),
            "STATE":fake.state(),
            "CREATED_AT":datetime.now(),
            "UPDATED_AT":datetime.now(),
            "SOURCE_SYSTEM":"CRM_V2"
               
        })
    
    return pd.DataFrame(customer)