import pandas as pd
import numpy as np

from pandas import DataFrame,Series
from datetime import datetime
from datetime import timedelta

import csv
import os


# 获取 qlife-knight
def qk_app(data):
    if "qlife-knight-android" in eval(data) or "qlife-knight-ios" in eval(data):
        return 1
    else:
        return 0
# 获取相应设备数目
def qk_device(data):
    if 'qlife-knight-android' in eval(data).keys() and 'qlife-knight-ios' in eval(data).keys():
        return  2
    elif 'qlife-knight-android' in eval(data).keys() :
        return 1
    elif 'qlife-knight-ios' in eval(data).keys() :
        return 1

# 生成所需文件
def spanned_file(all_update_data):
    update_data_test = all_update_data
    #引入判断条件
    update_data_test['qk_app'] = update_data_test['app_codes'].map(qk_app)
    update_data_test['qk_device'] = update_data_test['online_time'].map(qk_device)
    #Boss_knight_android操作文件
    qlife_knight =  update_data_test[update_data_test['qk_app']!=0]
    return qlife_knight

# 存储每日分时段在线人数
def save_csv(month,new_day,df):
    if list(df['在线人数'])[-6:] != [0,0,0,0,0,0]:
        if new_day ==  '01':
            path = os.path.join('../save_file/', '{}_TimeLine.csv'.format(month))
            list1 = list(df['时间段'])
            list1.insert(0, 'date')
            list2 = list(df['在线人数'])
            list2.insert(0, int(month+new_day))
            out_f = open(path,'a+',newline='')
            writer = csv.writer(out_f)
            writer.writerow(list1)
            writer.writerow(list2)
            out_f.close()
        else:
            path = os.path.join('../save_file/', '{}_TimeLine.csv'.format(month))
            list2 = list(df['在线人数'])
            list2.insert(0,int(month+new_day))
            out_f = open(path,'a+',newline='')
            writer = csv.writer(out_f)
            writer.writerow(list2)
            out_f.close()
    else:
        return False

def get_hour(data,time):
    sum_count = 0
    for i in range(len(data)):
        if  time  in [int(iter_var[:2]) for iter_var in eval(data.hour_minute_list[i])]:
            sum_count += 1
    return sum_count

if __name__ == '__main__':
    today_data = pd.read_csv('../data_file/20200203-20200204_account_data_statistics.csv')
    work_data = spanned_file(today_data)
    work_data = work_data[work_data['day'] == 20200203]
    work_data.reset_index(drop=True, inplace=True)
    new_test = work_data[work_data.is_new_account == True]
    new_test.reset_index(drop=True, inplace=True)
    time_list = [int('{}'.format(i)) for i in range(24)]#E87AFF
    # 各时间点点函数
    new_list = []
    online_list = []
    for i in time_list:
        new_list.append(get_hour(new_test,i))
    for i in time_list:
        online_list.append(get_hour(work_data, i))
    data = {
        '时间段':time_list,
        '在线人数':online_list,
        '新增人数':new_list
    }
    df = DataFrame(data,index=time_list)
    print(list(df['在线人数']))
    print(list(df['时间段']))

    work_data['month'] = work_data['day'].map(lambda x:str(x)[:6])
    month =  work_data['month'].unique()[-1]
    new_day = str(work_data['day'].unique()[-1])[-2:]
    save_csv(month,new_day,df)
