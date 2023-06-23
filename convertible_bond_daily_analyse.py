import datetime

import akshare as ak
import pandas as pd
from decimal import Decimal

ANALYSE_START_DATE = '20230101'
ANALYSE_END_DATE = '20230602'
VALUE_DATE_LIMIT_STR = '2020-01-01'
DELIST_DATE_LIMIT_STR = '2023-01-01'
DAILY_DATA_START_DATE = '2023-01-01'

PREMIUM_RATE_THRESHOLD = 10
CLOSE_THRESHOLD = 110


# 获取可转债基本信息
def get_cb_basic_data():
    # 债券代码	债券简称	申购日期	申购代码	申购上限	正股代码	正股简称	正股价	转股价	转股价值	债现价	转股溢价率
    # 原股东配售-股权登记日	原股东配售-每股配售额	发行规模	中签号发布日	中签率	上市时间
    bond_zh_cov_df = ak.bond_zh_cov()
    # 过滤目前可以交易的可转债
    filtered_bond_zh_cov_df1 = bond_zh_cov_df[bond_zh_cov_df['转股溢价率'].notnull()]
    return filtered_bond_zh_cov_df1


# 获取可转债日线数据
def get_cb_daily_trans_data(ts_code):
    if ts_code.startswith('12'):
        ts_code = 'sz' + ts_code
    else:
        ts_code = 'sh' + ts_code
    bond_zh_hs_cov_daily_df = ak.bond_zh_hs_cov_daily(symbol=ts_code)
    sorted_daily_data_df = bond_zh_hs_cov_daily_df.sort_values(by=['date'], ascending=[True])
    return sorted_daily_data_df


# 获取历史溢价率
def get_cb_premium_rate_daily(ts_code):
    bond_zh_cov_value_analysis_df = ak.bond_zh_cov_value_analysis(symbol=ts_code)
    sorted_daily_data_df = bond_zh_cov_value_analysis_df.sort_values(by=['日期'], ascending=[True])
    return sorted_daily_data_df


# 获取溢价率满足要求的债券交易信息
def get_satisfied_daily_trans_data_with_premium_rate(cb_daily_data_df):
    res_df = pd.DataFrame(columns=["日期",
                                   "收盘价",
                                   "纯债价值",
                                   "转股价值",
                                   "纯债溢价率",
                                   "转股溢价率"
                                   ])
    # 1. 过滤收盘价为空的交易记录
    filtered_cb_daily_data_df1 = cb_daily_data_df[cb_daily_data_df['收盘价'].notnull()]
    # 2. 过滤收盘价为 100 的交易记录
    filtered_cb_daily_data_df2 = filtered_cb_daily_data_df1[filtered_cb_daily_data_df1['收盘价'] != 100]
    # 3. 只取交易日期为 2023 年的记录
    filtered_cb_daily_data_df3 = filtered_cb_daily_data_df2[
        filtered_cb_daily_data_df2['日期'] >= datetime.datetime.strptime(DAILY_DATA_START_DATE, '%Y-%m-%d').date()]
    # 4. 过滤转股溢价率、价格满足条件的记录
    for index, row in filtered_cb_daily_data_df3.iterrows():
        premium_rate = row['转股溢价率']
        close = row['收盘价']
        if premium_rate <= PREMIUM_RATE_THRESHOLD and close <= CLOSE_THRESHOLD:
            res_df.loc[len(res_df)] = row
    return res_df


def run():
    # 1. 获取可转债基础信息列表, 并返回根据基本信息筛选出满足条件的股票
    cb_basic_df = get_cb_basic_data()
    bond_list_num = cb_basic_df.shape[0]
    print("===> 获取到可转债总数为: ", bond_list_num)
    # 2. 个债分析
    counter = 1
    final_res_df = pd.DataFrame(columns=["日期",
                                         "收盘价",
                                         "纯债价值",
                                         "转股价值",
                                         "纯债溢价率",
                                         "转股溢价率",
                                         "债券代码"])
    for index, row in cb_basic_df.iterrows():
        ts_code = row['债券代码']
        print("  ===> 提交: ", counter, "/", bond_list_num, "支转债进行分析, 债券代码 =", ts_code)
        counter += 1
        try:
            cb_daily_data_df = get_cb_premium_rate_daily(ts_code)
        except Exception as e:
            print("获取个债数据异常, 债券代码 =", ts_code, e)
            continue
        satisfied_daily_trans_data_df = get_satisfied_daily_trans_data_with_premium_rate(cb_daily_data_df)
        if len(satisfied_daily_trans_data_df) > 0:
            satisfied_daily_trans_data_df["债券代码"] = ts_code
            final_res_df = pd.concat([final_res_df, satisfied_daily_trans_data_df], ignore_index=True)
    final_res_df.to_excel('satisfied_daily_trans_data.xlsx', sheet_name='satisfied_daily_trans_data')


def run_from_local(path):
    final_res_df = pd.read_excel(path, index_col=0)
    # print(final_res_df.head())
    print("=======> 收盘价均值", final_res_df['收盘价'].mean())
    print("=======> 收盘价标准差", final_res_df['收盘价'].std())
    print("=======> 溢价率均值", final_res_df['转股溢价率'].mean())
    print("=======> 溢价率标准差", final_res_df['转股溢价率'].std())


if __name__ == '__main__':
    run()
    # run_from_local('satisfied_daily_trans_data.xlsx')

