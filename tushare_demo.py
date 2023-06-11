import os
import numpy as np
import tushare as ts
import gc
import json
import re
import requests
import pandas as pd
import time

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 1000000)  # 最多显示数据的行数
ts.set_token(' ')
pro = ts.pro_api()


# 获取可转债和正股对应的dataframe
def get_code_name():
    df_name = pro.cb_basic()
    rename_dict = {'ts_code': '转债代码', "bond_short_name": "转债简称", "issue_size": "发行规模", "remain_size": "剩余规模",
                   "stk_code": "正股代码", "stk_short_name": "正股简称", "conv_start_date": "转股起始日",
                   "conv_price": "最新转股价", "maturity_date": "到期时间",
                   "first_conv_price": "初始转股价", "conv_end_date": "转股到期日"}
    df_name.rename(columns=rename_dict, inplace=True)  # 命名
    return df_name


# 获取转债日线数据
def get_code_daily_date(ts_code):
    # 可转债日线数据
    df_daily_data = pro.cb_daily(ts_code=ts_code)
    rename_dict = {'ts_code': '转债代码', 'trade_date': '交易日期', 'close': '收盘价',
                   'open': '开盘价', 'high': '最高价', 'low': '最低价', 'pre_close': "前收盘价",
                   'change': '涨跌额', 'pct_chg': '涨跌幅', 'vol': '成交量', 'amount': '成交额'}
    df_daily_data.rename(columns=rename_dict, inplace=True)  # 命名
    # 去除重复列
    df_daily_data = df_daily_data.loc[:, ~df_daily_data.columns.duplicated()]
    df_daily_data.sort_values(by=['交易日期'], inplace=True)  # 升序排列

    # 获取转股价格
    df_price_chg = pro.cb_price_chg(ts_code=ts_code)
    rename_dict = {"ts_code": "转债代码", "bond_short_name": "转债简称",
                   "change_date": "交易日期", "convert_price_initial": "初始转股价格",
                   "convertprice_bef": "修正前转股价格", "convertprice_aft": "修正后转股价格"
                   }  # 【成交量单位 手】【 成交额单位 千】
    df_price_chg.rename(columns=rename_dict, inplace=True)  # 命名
    df_price_chg = df_price_chg[["转债代码", "转债简称", "交易日期", "初始转股价格", "修正前转股价格", "修正后转股价格"]]
    if df_price_chg.empty:
        df_data = df_daily_data
    else:
        df_price_chg.sort_values(by=['交易日期'], inplace=True)  # 升序排列
        df_daily_data["原始转股价格"] = df_price_chg["初始转股价格"][0]  # 由于初始转股价格产生与债券上市发行前，合并会丢失
        df_data = pd.merge(left=df_daily_data, right=df_price_chg, on=['交易日期', '转债代码'], how='left', sort=True,
                           indicator=True)
        df_data["修正前转股价格"] = (df_data["修正前转股价格"].fillna(method='ffill'))  # 向上寻找最近的一个非空值
        df_data["修正后转股价格"] = (df_data["修正后转股价格"].fillna(method='ffill'))  # 向上寻找最近的一个非空值
        df_data['修正前转股价格'].fillna(value=df_data['原始转股价格'], inplace=True)  # 用原始转股价补全未修正前的空值
        df_data['修正后转股价格'].fillna(value=df_data['原始转股价格'], inplace=True)  # 用原始转股价补全未修正前的空值
        df_data.drop(['_merge', '初始转股价格', '转债简称', '涨跌额'], axis=1,
                     inplace=True)  # inplace=True,) # TUS自带的涨跌幅是不能用的，必须删除然后自己计算 直接从内部删除
        df_data['涨跌幅'] = df_data['收盘价'] / df_data['前收盘价'] - 1
        df_data.reset_index(drop=True, inplace=True)
    return df_data


# 获取正股日线数据
def get_undercode_daily_data(stock_code):
    df_undercode_1 = pro.daily(ts_code=stock_code)
    rename_dict_1 = {'ts_code': '正股代码', 'trade_date': '交易日期', 'close': '正股收盘价',
                     'open': '正股开盘价', 'high': '正股最高价', 'low': '正股最低价', 'pre_close': "正股前收盘价",
                     'change': '正股涨跌额', 'pct_chg': '正股涨跌幅', 'vol': '正股成交量', 'amount': '正股成交额'}
    df_undercode_1.rename(columns=rename_dict_1, inplace=True)  # 命名
    # 去除重复列
    df_undercode_1 = df_undercode_1.loc[:, ~df_undercode_1.columns.duplicated()]
    # 转换单位
    df_undercode_1['正股成交量'] = df_undercode_1['正股成交量'] * 1000
    df_undercode_1['正股成交额'] = df_undercode_1['正股成交额'] * 1000
    df_undercode_1.sort_values(by=['交易日期'], inplace=True)  # 交易日期升序排列
    # 获取股票名称
    df_undercode_2 = pro.stock_basic(ts_code=stock_code, fields='ts_code,name')
    rename_dict_2 = {'ts_code': '正股代码', 'name': '正股名称'}
    df_undercode_2.rename(columns=rename_dict_2, inplace=True)  # 命名
    df_undercode_12 = pd.merge(df_undercode_2, df_undercode_1, on='正股代码', how='left')

    df_undercode_3 = pro.daily_basic(ts_code=stock_code, fields='ts_code,trade_date,turnover_rate_f,total_mv,circ_mv')
    rename_dict_3 = {'ts_code': '正股代码', 'trade_date': '交易日期', 'turnover_rate_f': '正股换手率',
                     'total_mv': '正股总市值', 'circ_mv': '正股流通市值'}
    df_undercode_3.rename(columns=rename_dict_3, inplace=True)  # 命名
    # 去除重复列
    df_undercode_3 = df_undercode_3.loc[:, ~df_undercode_3.columns.duplicated()]
    # 转换单位
    df_undercode_3['正股总市值'] = df_undercode_3['正股总市值'] * 10000
    df_undercode_3['正股流通市值'] = df_undercode_3['正股流通市值'] * 10000
    df_undercode_3.sort_values(by=['交易日期'], inplace=True)  # 交易日期升序排列
    df_undercode = pd.merge(df_undercode_12, df_undercode_3, on=['正股代码', '交易日期'], how='left')
    df_undercode.drop(['正股涨跌额'], axis=1, inplace=True)  # inplace=True,) # TUS自带的涨跌幅是不能用的，必须删除然后自己计算 直接从内部删除
    df_undercode['正股涨跌幅'] = df_undercode['正股收盘价'] / df_undercode['正股前收盘价'] - 1
    df_undercode.sort_values(by=['交易日期'], inplace=True)  # 升序排列
    return df_undercode


# 合并+计算溢价率+保存 删选2010年后的数据
def to_combine():
    df_0 = get_code_name()
    # 遍历df_0，获取所有可转债及对应正股的信息，并合并
    for i in df_0.index.tolist():
        # 获取可转债数据
        ts_code = df_0.loc[i, '转债代码']
        print(ts_code)
        df_1 = get_code_daily_date(ts_code)
        df_1['交易日期'] = pd.to_datetime(df_1['交易日期'], errors='coerce')
        df_1 = df_1[df_1['交易日期'] >= pd.to_datetime('20100101')]
        if df_1.empty:
            print('该转债数据早于2010年,不用收集')
            time.sleep(2)
        else:
            df_1.insert(1, '转债名称', value=df_0.loc[i, '转债简称'])
            df_1.insert(14, '到期时间', value=df_0.loc[i, '到期时间'])
            df_1.insert(15, '发行规模', value=df_0.loc[i, '发行规模'])
            df_1.insert(16, '剩余规模', value=df_0.loc[i, '剩余规模'])
        # 获取正股数据
        stock_code = df_0.loc[i, '正股代码']
        print(stock_code)
        df_undercode = get_undercode_daily_data(stock_code)

        # 数据合并
        df = pd.merge(df_1, df_undercode, on=['交易日期'], how='left')
        # 计算溢价率
        # 转股溢价率计算公式是：溢价率 = 可转债价格÷转股价值 - 1, 可转债的转股价值=可转债的正股价格÷可转债的转股价×100
        df['转股价值'] = df['正股收盘价'] / df['修正后转股价格'] * 100
        df['溢价率'] = df['收盘价'] / df['转股价值'] - 1
        # df['正股复权因子'] = (1 + df['正股涨跌幅']).cumprod()
        # df['正股收盘价_复权'] = df['正股复权因子'] * (df.iloc[0]['正股收盘价'] / df.iloc[0]['正股复权因子'])
        # df['复权因子'] = (1 + df['涨跌幅']).cumprod()
        # df['收盘价_复权'] = df['复权因子'] * (df.iloc[0]['收盘价'] / df.iloc[0]['复权因子'])
        # df["转股价值"] = 100 / df["修正后转股价格"] * df["正股收盘价_复权"]
        # df["溢价率"] = df['收盘价_复权'] / df["转股价值"] - 1
        df['溢价率'].fillna(value="nan", inplace=True)
        # 数据存储路径
        path = r'E:\stock/' + str(ts_code) + '.csv'
        pd.DataFrame(columns=['数据来源于Tushare']).to_csv(path, index=False, encoding='gbk')
        df.to_csv(path, index=False, encoding='gbk')
        time.sleep(2)
        to_combine()

        exit()
