import pandas as pd
import uuid
import numpy as np
from datetime import datetime,timedelta

def generate_deliveries(orders_df):
    deliveries=[]
    for _, order in orders_df.iterrows():
        deliveries.append({
            "DELIVERY_ID":str(uuid.uuid4()),
            "ORDER_ID":order["ORDER_ID"],
            "DELIVERY_STATUS":"DELIVERED",
            "DISPATCH_TIME": int(datetime.now().timestamp() * 1_000_000_000),
            "ACTUAL_DELIVERY_TIME": int((datetime.now() + timedelta(days=1)).timestamp() * 1_000_000_000),
            "CITY":order["CITY"],
            "SOURCE_SYSTEM":"TRACKING_SYS"
            
        })
        
    return pd.DataFrame(deliveries)