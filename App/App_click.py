import pandas as pd
import xlwt

# csv处理
def settle_table(df1,df2):
    key_num = list(set(df1['day'].unique()).intersection(set(df2['day'].unique())))
    df1 = df1[~df1['day'].isin(key_num)]
    df = pd.concat([df1,df2],axis=0,sort=True)
    df.to_csv('/Users/admin/PycharmProjects/untitled/click_file/half_part.csv',index=False)
    return df


# 存储文件
def test_file(df_file):
    df_file['日期'] = pd.to_datetime(df_file['日期'])
    df_file['month'] = df_file['日期'].dt.month
    if len(df_file['month'].unique()) == 2:
        save_file = df_file[df_file['month'] == df_file['month'].unique()[0]].reset_index(drop=True)
        save_file = save_file.drop(['month'], axis=1)
        save_file.to_csv('../接口月数据（合并设备）/2019_{}.csv'.format(df_file['month'].unique()[0]), index=False)
        jiekou_file = df_file[df_file['month'] == df_file['month'].unique()[1]].reset_index(drop=True)
        jiekou_file = jiekou_file.drop(['month'], axis=1)
        jiekou_file.to_csv('../接口月数据（合并设备）/new_half.csv', index=False)
    else:
        jiekou_file = df_file[df_file['month'] == df_file['month'].unique()[0]].reset_index(drop=True)
        jiekou_file = jiekou_file.drop(['month'], axis=1)
        jiekou_file.to_csv('../接口月数据（合并设备）/new_half.csv', index=False)
    return jiekou_file

def click_num(day,df,date):
    list_count=[]
    if day == 1:
        for num in date:
            inter_count = df[df['day']==num]
            list_count.append(len(inter_count))
    else:
        for num in date[:-day+1]:
            inter_count = df[df['day']==num]
            for count in range(1,day):
                inter_count = pd.merge(inter_count,df[df['day']==(num+count)],on='account_id')
            list_count.append(len(inter_count))
    return list_count

def get_data(len_num,dict_map,df,date):
    new_dict = {}
    new_dict['日期'] = list(date)
    for i in range(1,len_num+1):
        new_dict[dict_map[i]] = click_num(i,df,date)
    return new_dict

# 写入文件
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
    f.save('../save_file/click_cnt.xls')


if __name__ == '__main__':
    # 文件路径(变更月数据需要修改)
    path_1 = '/Users/admin/PycharmProjects/untitled/click_file/half_part.csv'
    path_2 = '/Users/admin/PycharmProjects/untitled/click_file/20200203-20200204_account_clock_log.csv'

    # 读取、整理文件
    df1 = pd.read_csv(path_1)
    df2 = pd.read_csv(path_2)
    df = settle_table(df1,df2)
    # test_file(df)
    print('本月至今，签到人数：{}'.format(len(df.account_id.unique())))
    date = pd.Series(df['day'].unique()).sort_values()
    # 准备工作
    day_map = [x+1 for x in range(len(date))]
    chn_map = ['当日签到' if x == 0 else '连续签到{}天'.format(x+1) for x in range(len(date))]
    dict_map = dict(zip(day_map,chn_map))
    dict_click = get_data(len(date),dict_map,df,date)

    #写入表格
    write_file(dict_click)
    # 待处理knn机器学习方法（暂时需要需要三周数据）
