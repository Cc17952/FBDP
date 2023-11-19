import pandas as pd
import numpy as np 

data = pd.read_csv('./data/application_data.csv')

data = data['TARGET']

with open('./data/Target.txt','a') as f:
    for i in data:
        print(i)
        f.write(str(i)+'\n')
        # f.write(i)