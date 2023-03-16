import pandas as pd
import json

with open('instaces.json', encoding='utf-8') as f:
   data = json.loads(f.read())
jsonBody = pd.json_normalize(data).rename(columns={'id': 'resource_id'})
labels_columns = [col for col in jsonBody.columns if "labels." in col]
labels_columns.append("resource_id")
jsonBody_subset = jsonBody[labels_columns]
df1 = pd.read_csv('20230213.csv')
merged_data = df1.merge(jsonBody_subset,on=["resource_id"],how="left")
merged_data.to_csv('merged.csv')   
print(merged_data)