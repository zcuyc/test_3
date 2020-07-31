import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import xlwt


'''每天点击活动人数'''
#信息暂时未知
'''进入提现的用户、发起提现的用户、提现成功的用户'''
def raffle_click(df1,df2):
    '''每天参与活动的人数-点击抽奖'''
    raffle_count = df_1.groupby(["day"]).agg({"account_id": "count", "account_id": pd.Series.nunique}).reset_index().rename(columns={'day':'日期','account_id':'点击抽奖人数'})
    '''每天参与活动两次，一次的人数'''
    # 下面这条信息得出来具体的那些人参与了几次抽奖活动
    raffle_day_count = df_1.groupby(['day','account_id'],as_index=False)['account_id'].agg({'_id':'count'}).rename(columns={'day':'日期','_id':'每日抽奖次数'})
    new_raffle_day_count = raffle_day_count[(raffle_day_count.每日抽奖次数==2) | (raffle_day_count.每日抽奖次数==3)]
    new_raffle_day_count = new_raffle_day_count.groupby(['日期'],as_index=False).agg({'account_id':'count'}).rename(columns={'account_id':'参与两次抽奖人数'})
    '''每日签到的人数'''
    click_count = df_2.groupby(["day"]).agg({"account_id": "count", "account_id": pd.Series.nunique}).reset_index().rename(columns={'day':'日期','account_id':'每日签到人数'})
    all_data = pd.merge(raffle_count,new_raffle_day_count,on='日期',how='left')
    all_data = pd.merge(all_data,click_count,on='日期')
    all_data = all_data.fillna(0)
    all_data['参与一次抽奖的人数'] = all_data['点击抽奖人数'] - all_data['参与两次抽奖人数']
    all_data.to_excel('../save_file/all_cnt.xls')
    return all_data

def click_num(day,df,date):
    list_count=[]
    if day == 1:
        for num in date:
            inter_count = df[df['day']==num]
            list_count.append(len(inter_count.drop_duplicates(subset='account_id', keep='first',inplace=False)))
    else:
        for num in date[:-day+1]:
            inter_count = df[df['day']==num]
            inter_count.drop_duplicates(subset='account_id', keep='first', inplace=False)
            for count in range(1,day):
                inter_count = pd.merge(inter_count,df[df['day']==(num+count)].drop_duplicates(subset='account_id', keep='first', inplace=False),on='account_id')
            list_count.append(len(inter_count))
    return list_count

def get_data(len_num,dict_map,df,date):
    new_dict = {}
    new_dict['日期'] = list(date)
    for i in range(1,len_num+1):
        new_dict[dict_map[i]] = click_num(i,df,date)
    return new_dict

def write_file(dict_click):
    f = xlwt.Workbook()  # 创建工作薄
    sheet1 = f.add_sheet(u'sheet1', cell_overwrite_ok=True)  # 创建sheet
    for i in range(len(list(dict_click.keys()))):
        sheet1.write(0, i, list(dict_click.keys())[i])
    for count in range(len(dict_click.values())):
        j = 1
        list_num = list(dict_click.values())[count]
        for i in list_num:
            sheet1.write(j, count, i)  # 循环写入 竖着写
            j = j + 1
    f.save('../save_file/active_cnt.xls')

'''每日抽奖抽中现金红包的人数、抽中现金红包的总额'''
# 钱数目多少合适设置预警线
def money_count(df_3):
    df_3['day'] = df_3.created_at.map(lambda x: x[0:10])
    money_detail = df_3.groupby(["day"]).agg(
        {"account_id": "count", 'money': 'sum', "account_id": pd.Series.nunique}).reset_index().rename(
        columns={'day': '日期', 'account_id': '中奖人数'})
    money_detail.to_csv('../save_file/money_detail.csv')
    return money_detail
# 计数去重，sum求和
'''连续参与活动的用户'''
# 类似于连续签到数据
if __name__ == '__main__':
    df_1 = pd.read_csv('../data_file/20200125_account_activity.csv')
    df_2 = pd.read_csv('../click_file/half_part.csv')
    df_3 = pd.read_csv('../data_file/20200125_account_activity_reward.csv')
    all_data = raffle_click(df_1,df_2)
    df_1 = df_1[df_1['day']>= 20200102]
    print('本月至今，参加活动人数：{}'.format(len(df_1[df_1['month'] == list(df_1['month'])[-1]].account_id.unique())))

# 准备工作
    date = pd.Series(df_1['day'].unique()).sort_values()
    day_map = [x + 1 for x in range(len(date))]
    chn_map = ['当日参加活动' if x == 0 else '连续参加{}天'.format(x + 1) for x in range(len(date))]
    dict_map = dict(zip(day_map, chn_map))
    dict_click = get_data(len(date), dict_map, df_1, date)
    write_file(dict_click)
    money_detail =  money_count(df_3)









