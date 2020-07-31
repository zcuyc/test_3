import pandas as pd
import os

def merge_data(read_path,fin_name,file_type):
    file_list = [os.path.join(read_path, x) for x in os.listdir(read_path)]
    df = pd.read_csv(file_list[0])
    for i in range(1, len(file_list)):
            df_next = pd.read_csv(file_list[i])
            df = pd.concat([df, df_next],sort=False)
    # df.to_csv('{}{}.{}'.format(save_path, fin_name, file_type), encoding='utf-8-sig', index=False)
    return df
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
    else:
        return 0

read_path = '/Users/admin/PycharmProjects/untitled/data_file_num'
# save_path = '/Users/admin/PycharmProjects/untitled/历史全量App数据'
fin_name = '7月app月数据'
file_type = 'csv'
# 执行合并函数


# 计算账号登陆人数
df_merge_file = merge_data(read_path,fin_name,file_type)
df_merge_file['qk_app'] = df_merge_file['app_codes'].map(qk_app)
df_merge_file['qk_device'] = df_merge_file['online_time'].map(qk_device)
#Boss_knight_android操作文件
qlife_knight =  df_merge_file[df_merge_file['qk_app']!=0]
print('当月账号登陆人数(月活去重)：{}人'.format(len(qlife_knight['account_id'].unique())))
# 设备路径人数统计
qlife_knight_sort = qlife_knight.sort_values(by=['account_id','qk_device'],ascending=False)
qlife_knight_count = qlife_knight_sort.drop_duplicates(subset=['account_id'],keep='first',inplace=False)
print('当月设备登陆人数(月活去重)：{}人'.format(qlife_knight_count['qk_device'].sum()))
# df.loc['5', 'A']