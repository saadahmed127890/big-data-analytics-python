import pandas as pd

# Load datasets
retail = pd.read_csv("retail_events.csv")
bank = pd.read_csv("bank_txn.csv")
iot = pd.read_csv("iot_air.csv")

# Parse timestamps
retail["event_ts"] = pd.to_datetime(retail["event_ts"])
bank["txn_ts"] = pd.to_datetime(bank["txn_ts"])
iot["reading_ts"] = pd.to_datetime(iot["reading_ts"])

# ---------------- Retail funnel + revenue KPIs ----------------
counts = retail["event_type"].value_counts()

views = counts.get("view", 0)
adds = counts.get("add_to_cart", 0)
checkouts = counts.get("checkout", 0)
purchases = counts.get("purchase", 0)

revenue = retail.loc[retail["event_type"] == "purchase", "price"].sum()
refunds = retail.loc[retail["event_type"] == "refund", "price"].sum()

conversion = purchases / views if views else 0
cart_abandonment = (adds - purchases) / adds if adds else 0
checkout_dropoff = (checkouts - purchases) / checkouts if checkouts else 0
aov = revenue / purchases if purchases else 0
refund_rate = refunds / revenue if revenue else 0

retail_kpis = {
    "views": int(views),
    "add_to_cart": int(adds),
    "checkouts": int(checkouts),
    "purchases": int(purchases),
    "revenue": float(revenue),
    "conversion_rate": float(conversion),
    "cart_abandonment": float(cart_abandonment),
    "checkout_dropoff": float(checkout_dropoff),
    "aov": float(aov),
    "refund_rate": float(refund_rate)
}

print("Retail KPIs:")
print(retail_kpis)

# Revenue by country
rev_by_country = (
    retail.loc[retail["event_type"] == "purchase"]
    .groupby("country")["price"]
    .sum()
    .sort_values(ascending=False)
)

print("\nRevenue by Country:")
print(rev_by_country)

# ---------------- IoT anomaly metrics ----------------
THRESH = 80
iot["is_anomaly"] = (iot["pm25"] >= THRESH).astype(int)

anomaly_rate = float(iot["is_anomaly"].mean())
top_sensors = iot.groupby("sensor_id")["is_anomaly"].sum().sort_values(ascending=False)

print("\nIoT Anomaly Rate:")
print(anomaly_rate)

print("\nTop Sensors by Anomalies:")
print(top_sensors.head(3))

# ---------------- Banking risk score + evaluation ----------------
bank["risk_score"] = 0
bank.loc[bank["amount"] >= 1000, "risk_score"] += 2
bank.loc[bank["is_international"] == 1, "risk_score"] += 2
bank.loc[bank["merchant_category"].isin(["electronics", "travel"]), "risk_score"] += 1

bank["alert"] = (bank["risk_score"] >= 3).astype(int)

tp = int(((bank["alert"] == 1) & (bank["label_is_fraud"] == 1)).sum())
fp = int(((bank["alert"] == 1) & (bank["label_is_fraud"] == 0)).sum())
tn = int(((bank["alert"] == 0) & (bank["label_is_fraud"] == 0)).sum())
fn = int(((bank["alert"] == 0) & (bank["label_is_fraud"] == 1)).sum())

precision = tp / (tp + fp) if (tp + fp) else 0.0
recall = tp / (tp + fn) if (tp + fn) else 0.0
false_positive_rate = fp / (fp + tn) if (fp + tn) else 0.0

bank_eval = {
    "tp": tp,
    "fp": fp,
    "tn": tn,
    "fn": fn,
    "precision": float(precision),
    "recall": float(recall),
    "false_positive_rate": float(false_positive_rate)
}

print("\nBank Evaluation:")
print(bank_eval)