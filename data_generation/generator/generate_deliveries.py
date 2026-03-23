import pandas as pd
import uuid
import numpy as np
from datetime import datetime,timedelta
import random
delivery_status=["Dispatched","In_Transit","Delivered","Failed"]
def generate_deliveries(orders_df):
    n=len(orders_df)
    new_count=int(n*0.7)
    update_count=int(n*0.2)
    duplicates_count=n-new_count-update_count
    deliveries=[]
    sampled_orders=orders_df.sample(new_count)
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
        
    deliveries_df= pd.DataFrame(deliveries)
    
    #updated deliveries
    updates=deliveries_df.sample(update_count).copy()
    updates["DELIVERY_STATUS"] = random.choice(
        ["IN_TRANSIT","DELIVERED","FAILED"]
    )

    updates["ACTUAL_DELIVERY_TIME"] = int(
        datetime.now().timestamp() * 1_000_000_000
    )
    #  DUPLICATES
    duplicates = deliveries_df.sample(duplicates_count).copy()

    #  COMBINE
    final_deliveries = pd.concat(
        [deliveries_df, updates, duplicates],
        ignore_index=True
    )

    return final_deliveries