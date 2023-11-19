import pandas as pd
import numpy as np 

data = pd.read_csv('./data/application_data.csv')

data = data['WEEKDAY_APPR_PROCESS_START']

with open('./data/Weekday.txt','w') as f:
    for i in data:
        print(i)
        f.write(str(i)+'\n')
        # f.write(i)