import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import  os

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

# 计算基本指标
def basic_index(update_data):
    # 日已存在用户量
    day_old_account = update_data[update_data.is_new_account == False].groupby(['day']).agg({'account_id': 'count'})
    day_old_account.reset_index(inplace=True)
    day_old_account = day_old_account.rename(columns={'account_id': '日已存在用户量'})
    # 日新增用户量
    day_new_account = update_data[update_data.is_new_account == True].groupby(['day']).agg({'account_id': 'count'})
    day_new_account.reset_index(inplace=True)
    day_new_account = day_new_account.rename(columns={'account_id': '日新增用户量'})
    # 日活跃量
    day_active_account = update_data.groupby(['day']).agg({'account_id': 'count'})
    day_active_account.reset_index(inplace=True)
    day_active_account = day_active_account.rename(columns={'account_id': '日活跃用户量'})
    # 日使用次数
    day_use_account = update_data.groupby(['day']).agg({'open_app_count': 'sum'})
    day_use_account.reset_index(inplace=True)
    day_use_account = day_use_account.rename(columns={'open_app_count': '日使用次数'})
    # 日在线时长
    day_online_account = update_data.groupby(['day']).agg({'qk_device': 'sum'})
    day_online_account.reset_index(inplace=True)
    day_online_account = day_online_account.rename(columns={'qk_device': '设备活跃'})
    all_concat_1 = pd.merge(day_old_account, day_new_account, how='outer', on='day')
    all_concat_2 = pd.merge(day_active_account, day_use_account, how='outer', on='day')
    all_concat = pd.merge(all_concat_1, all_concat_2, how='outer', on='day')
    all_concat = pd.merge(all_concat, day_online_account, how='outer', on='day')
    all_concat = all_concat.rename(columns={'day': '日期'})
    all_concat['日期'] = all_concat['日期'].map(lambda x: datetime.strptime(str(x), '%Y%m%d'))
    # 添加月份
    all_concat = all_concat.sort_values(by='日期')
    all_concat['日期'] = all_concat['日期'].apply(lambda x: x.strftime('%Y-%m-%d'))
    all_concat = all_concat[['日期', '日活跃用户量', '日新增用户量', '设备活跃']]
    all_concat['设备活跃'] = all_concat['设备活跃'].astype('int')
    all_concat = all_concat.fillna(0)
    return all_concat

# 存储文件
def test_file(df_file):
    df_file['日期'] = pd.to_datetime(df_file['日期'])
    year_time = df_file['日期'].dt.year
    df_file['month'] = df_file['日期'].dt.month
    if len(df_file['month'].unique()) == 2:
        if len(df_file[df_file['month'] == df_file['month'].unique()[1]]) >= 7:
            save_file = df_file[df_file['month'] == df_file['month'].unique()[0]].reset_index(drop=True)
            save_file = save_file.drop(['month'], axis=1)
            save_file.to_csv('../历史文件/{}_{}.csv'.format(year_time[1],df_file['month'].unique()[0]), index=False)
            jiekou_file = df_file[df_file['month'] == df_file['month'].unique()[1]].reset_index(drop=True)
            jiekou_file = jiekou_file.drop(['month'], axis=1)
            jiekou_file = jiekou_file.drop_duplicates(subset=['日期'], keep='first')
            jiekou_file.to_csv('../接口月数据（合并设备）/jiekou_using.csv', index=False)
        else:
            jiekou_file = df_file.reset_index(drop=True)
            jiekou_file = jiekou_file.drop(['month'], axis=1)
            jiekou_file = jiekou_file.drop_duplicates(subset=['日期'], keep='first')
            jiekou_file.to_csv('../接口月数据（合并设备）/jiekou_using.csv', index=False)
    else:
        jiekou_file = df_file[df_file['month'] == df_file['month'].unique()[0]].reset_index(drop=True)
        jiekou_file = jiekou_file.drop(['month'], axis=1)
        jiekou_file = jiekou_file.drop_duplicates(subset=['日期'], keep='first')
        jiekou_file.to_csv('../接口月数据（合并设备）/jiekou_using.csv', index=False)
    return jiekou_file

# 友盟数据
def uapp_file(uapp_android,uapp_ios):
    uapp_android = uapp_android[['日期', '新增用户', '活跃用户', '启动次数', '累计用户']]
    uapp_data = pd.concat([uapp_android, uapp_ios], sort=True)
    uapp_data = uapp_data.groupby(['日期']).sum().reset_index()
    uapp_data['日期'] = uapp_data['日期'].map(lambda x: datetime.strptime(str(x), '%Y-%m-%d'))
    uapp_data = uapp_data.sort_values(by='日期').reset_index(drop=True)
    uapp_data.to_csv('../data_file/uapp_data.csv', index=False)
    return uapp_data

# 合并
def uapp_jiekou(jiekou_file,uapp_data):
    jiekou_data = jiekou_file.iloc[-8:, :].reset_index(drop=True)
    uapp_data = uapp_data[['日期', '新增用户', '活跃用户']]
    jiekou_data = jiekou_data[['日期', '日新增用户量', '日活跃用户量', '设备活跃']]
    if (uapp_data['日期'] == jiekou_data['日期']).any():
        jiekou_data = jiekou_data[['日新增用户量', '日活跃用户量', '设备活跃']]
        uapp_jiekou = pd.concat([uapp_data, jiekou_data], axis=1)
        uapp_jiekou = uapp_jiekou.rename(columns={'新增用户': '友盟新增', '活跃用户': '友盟活跃', '日新增用户量': '接口新增', '日活跃用户量': '接口活跃'})
        uapp_jiekou.eval("活跃差量 = 友盟活跃-接口活跃", inplace=True)
        uapp_jiekou.eval("新增差量 = 友盟新增-接口新增", inplace=True)
        uapp_jiekou = uapp_jiekou[['日期', '友盟活跃', '友盟新增', '设备活跃', '接口活跃', '接口新增', '活跃差量', '新增差量']]
        uapp_jiekou.to_csv('../day_update.csv', index=False)
        return uapp_jiekou
    else:
        return '日期不对称'

if __name__ == '__main__':
    root_path = '../data_file'
    file_list = ([os.path.join(root_path,x) for x in os.listdir(root_path)])
    new_file_list = []
    for i in file_list:
        if i == "../data_file/.DS_Store":
            continue
        else:
            new_file_list.append(i)
    new_file_list.sort()
    for i in  new_file_list:
    # # # 接口数据
        all_update_data = pd.read_csv(i)
        part_merge = basic_index(spanned_file(all_update_data))
        df_file = pd.read_csv('../接口月数据（合并设备）/jiekou_using.csv')
        part_merge = pd.concat([df_file.iloc[:-1,:],part_merge],axis=0).sort_values(by='日期')
        part_merge.reset_index(drop=True, inplace=True)
        jiekou_file = test_file(part_merge)

    # root_path = '../data_file'
    # print([os.path.join(root_path,x) for x in os.listdir(root_path)])
    # print(pd.read_csv([os.path.join(root_path,x) for x in os.listdir(root_path)][1]))
    # 友盟数据
    # uapp_android = pd.read_csv('../data_file/BOSS骑士_Android_Release_整体趋势_20200127_20200203.csv')
    # uapp_ios = pd.read_csv('../data_file/BOSS骑士_IOS_Release_整体趋势_20200127_20200203.csv')
    # uapp_data = uapp_file(uapp_android,uapp_ios)
    # # 友盟-接口合并
    # uapp_jiekou = uapp_jiekou(jiekou_file,uapp_data)


