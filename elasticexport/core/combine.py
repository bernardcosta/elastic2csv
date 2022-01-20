import pandas as pd


tracking = pd.read_csv("sg_results_tracking_2021.csv")
partner = pd.read_csv("sg_results_partner_2021.csv")


data = pd.merge(tracking, partner, on="url", how='left')

print(data.columns)
data['revenue_y'] = data['revenue_y'].fillna(0)
data['total_rev'] = data['revenue_x'] + data['revenue_y']

print(data.head())

data.to_csv("sg_2021.csv", index=False)
