import pandas as pd 
import numpy as np
import uuid
from datetime import datetime
import random


payment_mode=["UPI","CARD","COD"]

def generate_payments(order_df):
    payments=[]
    for _, order in order_df.iterrows():
        now_ns = int(datetime.now().timestamp() * 1_000_000_000)
        payments.append({
            "PAYMENT_ID":str(uuid.uuid4()),
            "ORDER_ID":order["ORDER_ID"],
            "PAYMENT_MODE":random.choice(payment_mode),
            "PAYMENT_STATUS":"SUCCESS",
            "AMOUNT":order["ORDER_AMOUNT"],
            "PAYMENT_TIMESTAMP":now_ns,
            "SOURCE_SYSTEM":"PAYMENT_GATEWAY"
        })
        
    return pd.DataFrame(payments)   