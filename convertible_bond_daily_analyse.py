import akshare as ak
import pandas as pd
from decimal import Decimal

ANALYSE_START_DATE = '20230101'
ANALYSE_END_DATE = '20230602'
VALUE_DATE_LIMIT_STR = '2020-01-01'
DELIST_DATE_LIMIT_STR = '2023-01-01'


# 获取可转债基本信息
def get_cb_basic_df():
    # 债券代码	债券简称	申购日期	申购代码	申购上限	正股代码	正股简称	正股价	转股价	转股价值	债现价	转股溢价率
    # 原股东配售-股权登记日	原股东配售-每股配售额	发行规模	中签号发布日	中签率	上市时间
    bond_zh_cov_df = ak.bond_zh_cov()
    # 过滤目前可以交易的可转债
    filtered_bond_zh_cov_df1 = bond_zh_cov_df[bond_zh_cov_df['转股溢价率'].notnull()]
    return filtered_bond_zh_cov_df1


def get_cb_daily_data(ts_code):
    # 可转债日线数据
    bond_zh_hs_cov_daily_df = ak.bond_zh_hs_cov_daily(symbol=ts_code)
    sorted_daily_data_df = bond_zh_hs_cov_daily_df.sort_values(by=['date'], ascending=[True])
    return bond_zh_hs_cov_daily_df


def run():
    # 1. 获取可转债基础信息列表, 并返回根据基本信息筛选出满足条件的股票
    cb_basic_df = get_cb_basic_df()
    bond_list_num = cb_basic_df.shape[0]
    print("===> 获取到可转债总数为: ", bond_list_num)
    # 2. 日行情分析
    counter = 1
    for index, row in cb_basic_df.iterrows():
        ts_code = row['债券代码']
        if ts_code.startswith('12'):
            ts_code = 'sz' + ts_code
        else:
            ts_code = 'sh' + ts_code
        print("  ===> 提交: ", counter, "/", bond_list_num, "支转债进行分析, 债券代码 =", ts_code)
        counter += 1
        try:
            cb_daily_data_df = get_cb_daily_data(ts_code)
            print(cb_daily_data_df)
            break
        except Exception as e:
            print("获取历史交易数据异常, 债券代码 =", ts_code, e)


if __name__ == '__main__':
    run()

    # 历史溢价率获取
    # import akshare as ak
    #
    # bond_zh_cov_value_analysis_df = ak.bond_zh_cov_value_analysis(symbol="113527")
    # print(bond_zh_cov_value_analysis_df)