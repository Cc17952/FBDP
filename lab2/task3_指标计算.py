import numpy as np
import pandas as pd 
import re 

data = ""
label_prediction = []
label_real = []
with open("./data/output_task3/part-r-00000", "r", encoding='utf-8') as f:  #打开文本
    for line in f:
        # print(line[-2:-1])
        # print(line[-9:-8])
        label_prediction.append(line[-2:-1])
        label_real.append(line[-9:-8])
        
f.close()

TP = 0
FN =0
TN = 0
FP= 0

for i in range(len(label_prediction)):
    prediction = label_prediction[i]
    real = label_real[i]
    if real =='1':
        if prediction == '1':
            TP+=1
        else:
            FN+=1
    else:
        if prediction=='1':
            FP+=1
        else:
            TN+=1

precision = float(TP)/(TP+FP)
recall = float(TP)/(TP+FN)
accuracy = float(TP+TN)/(TP+TN+FP+FN)
f1_score = 2*precision*recall/(precision+recall)


# 以违约作为本类的相关指标如下：
print("precision:"+str(precision))
print("accuracy:"+str(accuracy))
print("recall:"+str(recall))
print("f1_score:"+str(f1_score))