import pandas as pd
import json
from cherrypicker import CherryPicker

with open('instaces.json', encoding='utf-8') as f:
   data = json.loads(f.read())
jsonBody = pd.json_normalize(data).rename(columns={'id': 'resource_id'})
picker = CherryPicker(jsonBody)
jsonpicked = picker['resource_id', 'city*'].get()
print (picker)
df1 = pd.read_csv('20230213.csv')
merged_data = df1.merge(jsonBody,on=["resource_id"],how="left")
merged_data.to_csv('merged.csv')