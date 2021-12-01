import json
import numpy as np
import pandas as pd

# load the json file:
with open('kym.json','r') as f:
    data = json.loads(f.read())

# import the json file to the pandas df
df2 = pd.json_normalize(data,max_level=2)

# shape of the df is enourmous
print(df2.shape)

# look the columnnames.. columns "content._" are noisy!
list(df2.columns)