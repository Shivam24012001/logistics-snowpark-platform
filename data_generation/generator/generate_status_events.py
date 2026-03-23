import pandas as pd
import uuid
from datetime import datetime
import random

status_flow = ["PLACED","PACKED","SHIPPED","IN_TRANSIT","DELIVERED"]

def generate_status_events(orders_df):

    events = []

    for _, order in orders_df.iterrows():

        previous_status = None

        for status in status_flow:

            now_ns = int(datetime.now().timestamp() * 1_000_000_000)

            events.append({
                "EVENT_ID": str(uuid.uuid4()),
                "ORDER_ID": order["ORDER_ID"],
                "PREVIOUS_STATUS": previous_status,
                "CURRENT_STATUS": status,
                "EVENT_TIMESTAMP": now_ns,
                "EVENT_SOURCE": "OMS_EVENT"
            })

            previous_status = status

    events_df = pd.DataFrame(events)

    # 20% updated events
    update_count = int(len(events_df) * 0.2)
    updates = events_df.sample(update_count).copy()

    updates["CURRENT_STATUS"] = random.choice(["SHIPPED","IN_TRANSIT","DELIVERED"])
    updates["EVENT_TIMESTAMP"] = int(datetime.now().timestamp() * 1_000_000_000)

    # 10% duplicates
    duplicate_count = int(len(events_df) * 0.1)
    duplicates = events_df.sample(duplicate_count).copy()

    final_events = pd.concat(
        [events_df, updates, duplicates],
        ignore_index=True
    )

    return final_events