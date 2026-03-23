import pandas as pd
import numpy as np
import uuid
from datetime import datetime
import random

payment_modes = ["UPI","CARD","COD"]
payment_statuses = ["SUCCESS","FAILED"]

def generate_payments(order_df):

    n = len(order_df)

    new_count = int(n * 0.7)
    update_count = int(n * 0.2)
    duplicate_count = n - new_count - update_count

    payments = []

    sampled_orders = order_df.sample(new_count)

    # 1️⃣ New Payments
    for _, order in sampled_orders.iterrows():

        now_ns = int(datetime.now().timestamp() * 1_000_000_000)

        payments.append({
            "PAYMENT_ID": str(uuid.uuid4()),
            "ORDER_ID": order["ORDER_ID"],
            "PAYMENT_MODE": random.choice(payment_modes),
            "PAYMENT_STATUS": random.choice(payment_statuses),
            "AMOUNT": order["ORDER_AMOUNT"],
            "PAYMENT_TIMESTAMP": now_ns,
            "SOURCE_SYSTEM": "PAYMENT_GATEWAY"
        })

    payment_df = pd.DataFrame(payments)

    # 2️⃣ Updates (simulate refund or status change)
    updates = payment_df.sample(update_count).copy()

    updates["PAYMENT_STATUS"] = random.choice(["REFUNDED","FAILED"])

    updates["PAYMENT_TIMESTAMP"] = int(datetime.now().timestamp() * 1_000_000_000)

    # 3️⃣ Duplicates
    duplicates = payment_df.sample(duplicate_count).copy()

    # 4️⃣ Combine
    final_payments = pd.concat(
        [payment_df, updates, duplicates],
        ignore_index=True
    )

    return final_payments