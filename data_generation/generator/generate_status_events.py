import pandas as pd
import uuid
from datetime import datetime

def generate_status_events(orders_df):

    events = []

    for _, order in orders_df.iterrows():

        events.append({
            "EVENT_ID": str(uuid.uuid4()),
            "ORDER_ID": order["ORDER_ID"],
            "PREVIOUS_STATUS": "PLACED",
            "CURRENT_STATUS": "SHIPPED",
            "EVENT_TIMESTAMP": datetime.now(),
            "EVENT_SOURCE": "OMS_EVENT"
        })

    return pd.DataFrame(events)