import pandas as pd

df = pd.read_csv("location/towns.csv") 
only = df[['place_id','city', 'region_name','lat','lon']]

print(only.head(-5))