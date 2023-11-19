import os
import pandas as pd
import numpy as np
import warnings
from tqdm import tqdm
import re
from sklearn.preprocessing import MinMaxScaler # min-max 标准化

data = pd.read_csv('./data/application_data.csv')

# 选取部分可能相关的特征
# 对特征做归一化处理
# 保留需要的id 区分测试集和训练集

# ['FLAG_CONT_MOBILE','AMT_INCOME_TOTAL','AMT_CREDIT','FLAG_OWN_CAR','FLAG_OWN_REALTY','REGION_RATING_CLIENT','OBS_30_CNT_SOCIAL_CIRCLE']
data.dropna(inplace=True)
data2 = data[['SK_ID_CURR','FLAG_CONT_MOBILE','AMT_INCOME_TOTAL','AMT_CREDIT','FLAG_OWN_CAR','FLAG_OWN_REALTY','REGION_RATING_CLIENT','OBS_30_CNT_SOCIAL_CIRCLE']]
target_label = data[['SK_ID_CURR','TARGET']]

xVars = ['FLAG_CONT_MOBILE','AMT_INCOME_TOTAL','AMT_CREDIT','FLAG_OWN_CAR','FLAG_OWN_REALTY','REGION_RATING_CLIENT','OBS_30_CNT_SOCIAL_CIRCLE']
yVar = ['SK_ID_CURR','TARGET']

specialVars = ['FLAG_OWN_CAR','FLAG_OWN_REALTY']
for i in specialVars:
    # print(i)
    data2.replace({i:{'Y':1,'N':0}},inplace=True)
    # print(data2[i])
    

total_number = len(data2)
per = 0.8
train_number = int(per*total_number)
# test_number = int((1-per)*total_number)
# 如果直接转int total_number 可能不等于 train_number + test_number
test_number = total_number - train_number

train_data = data2.head(train_number)
train_label = target_label.head(train_number)
test_data = data2.tail(test_number)
test_label = target_label.tail(test_number)

print(test_data.columns)

train_data[xVars] = MinMaxScaler().fit_transform(train_data[xVars])
test_data[xVars] = MinMaxScaler().fit_transform(test_data[xVars])
train = pd.merge(train_data,train_label,on = 'SK_ID_CURR',how='inner')
test_label = test_label['TARGET']

train.to_csv('./data/train.csv',sep = ',',header = False,index_label=None,index=False)
test_data.to_csv('./data/test_data.csv',sep = ',',header = False,index_label=None,index=False)
test_label.to_csv("./data/test_label.csv",sep = ',',header = False,index_label=None,index=False)