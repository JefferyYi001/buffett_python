import concurrent.futures
import random
import time

import pandas as pd
import tushare as ts
from decimal import Decimal

# TOKEN = '8a5af498224fb2ebea8a11345fb4cfc81242631f66c7eebce8cdc055'
# TOKEN = 'cecc095f035b006972612fab8539c864f2bad50f41eff04ab6f33b91'
TOKEN = '591e6891f9287935f45fc712bcf62335a81cd6829ce76c21c0fdf7b2'
START_DATE = 20230101
END_DATE = 20230621
MAX_WORKERS = 1
THREAD_TIMEOUT_IN_SEC = 600
ANALYSE_WINDOW_LENGTH = 100


def get_satisfied_stocks(pro):
    all_stock_df = pro.stock_basic(**{
        "ts_code": "",
        "name": "",
        "exchange": "",
        "market": "",
        "is_hs": "",
        "list_status": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "name",
        "market",
        "symbol",
        "list_status"
    ])
    # list_status: 上市状态 L上市 D退市 P暂停上市
    # filtered_stock_df = all_stock_df[all_stock_df['list_status'] == 'L']
    # market: CDR, 北交所, 创业板, 科创板, 中小板, 主板
    filtered_stock_df1 = all_stock_df[all_stock_df['market'].isin(['主板', '中小板'])]
    # 过滤掉 ST 股票
    filtered_stock_df2 = filtered_stock_df1[~filtered_stock_df1['name'].str.contains('ST')]
    return filtered_stock_df2


# 涨停分析
def limit_up_analyse(num, total, sorted_daily_trans_df):
    res_df = pd.DataFrame(columns=['ts_code', 'trade_date', 'pct_change', 'close', 'change', 'open', 'high', 'low',
                                   'pre_close', 'vol_ratio', 'turn_over', 'vol', 'amount', "total_share", "float_share",
                                   'launch_date', 'grow_up_days'])
    date_window = []
    close_price_window = []
    for index, row in sorted_daily_trans_df.iterrows():
        # 涨停条件: 昨日收盘价 * 1.1 == 今日收盘价四舍五入到分
        pre_close_price = row['pre_close']
        close_price = row['close']
        trade_date = row['trade_date']
        limit_up_price_decimal = Decimal(str(pre_close_price * 1.1)).quantize(Decimal("0.01"), rounding="ROUND_HALF_UP")
        close_decimal = Decimal(str(close_price)).quantize(Decimal("0.01"), rounding="ROUND_HALF_UP")
        if limit_up_price_decimal == close_decimal and len(date_window) == 0:  # 首次涨停
            date_window.append(trade_date)
            close_price_window.append(close_price)
        elif 0 < len(date_window) < ANALYSE_WINDOW_LENGTH and close_price >= 1.2 * close_price_window[0]:
            row['launch_date'] = date_window[0]
            row['grow_up_days'] = len(date_window)
            res_df.loc[len(res_df)] = row
            break
        elif 0 < len(date_window) < ANALYSE_WINDOW_LENGTH:
            date_window.append(trade_date)
            close_price_window.append(close_price)
        elif len(date_window) >= ANALYSE_WINDOW_LENGTH:
            break
        # 2 日连续涨停逻辑
        #     window.append(trade_date)
        #     if len(window) > 1:
        #         res_df.loc[len(res_df)] = row
        # elif len(window) > 0:  # 当日不涨停则重置窗口
        #     window = []

    print("  ===> 完成第: ", num, "/", total, "支股票的分析")
    return res_df


# 基本交易数据分析
def basic_analyse(num, total, daily_basic_df, pro, ts_code):
    res_df = pd.DataFrame(columns=['ts_code', 'trade_date', 'pct_change', 'close', 'change', 'open', 'high', 'low',
                                   'pre_close', 'vol_ratio', 'turn_over', 'vol', 'amount', "total_share", "float_share",
                                   'highest_10_days', 'highest_10_days_pct', 'lowest_10_days', 'lowest_10_days_pct',
                                   'winner_rate'])
    for index, row in daily_basic_df.iterrows():
        pct_change = row['pct_change']
        vol_ratio = row['vol_ratio']
        turn_over = row['turn_over']
        float_share = row['float_share']
        close = row['close']
        trade_date = row['trade_date']

        # 1. 流通市值 10亿~150亿
        if 10 <= float_share <= 150:
            # 2. 涨幅 2%~6%
            # 3. 量比 >= 1
            # 4. 换手率 2%~7%
            if (2 <= pct_change <= 6) and (vol_ratio >= 1) and (2 <= turn_over <= 7):
                # 5. 计算接下来的 10 个交易日最高价与最高涨幅
                highest_10_days = daily_basic_df['high'].iloc[index: index + 10].max().item()
                row['highest_10_days'] = highest_10_days
                row['highest_10_days_pct'] = (highest_10_days - close) / close * 100
                # 6. 计算接下来的 10 个交易日最低价与最高回撤
                lowest_10_days = daily_basic_df['high'].iloc[index: index + 10].min().item()
                row['lowest_10_days'] = lowest_10_days
                row['lowest_10_days_pct'] = (lowest_10_days - close) / close * 100

                # 7. 获取当日筹码分布信息
                cyq_perf_df = pro.cyq_perf(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
                row['winner_rate'] = cyq_perf_df['winner_rate'].iloc[0]
                res_df.loc[len(res_df)] = row
        else:
            break
    time.sleep(random.random() * 2)
    print("  ===> 完成第: ", num, "/", total, "支股票的分析")
    return res_df


def get_daily_trans_and_analyse(num, total, pro, ts_code):
    daily_trans_df = pro.bak_daily(**{"ts_code": ts_code,
                                      "trade_date": "",
                                      "start_date": START_DATE,
                                      "end_date": END_DATE,
                                      "offset": "",
                                      "limit": ""
                                      },
                                   fields=['ts_code', 'trade_date', 'pct_change', 'close', 'change', 'open', 'high',
                                           'low', 'pre_close', 'vol_ratio', 'turn_over', 'vol', 'amount', "total_share",
                                           "float_share"])
    daily_trans_df['launch_date'] = 0
    daily_trans_df['grow_up_days'] = 0
    sorted_daily_trans_df = daily_trans_df.sort_values(by=['trade_date'], ascending=[True]).reset_index(drop=True)
    # limit_up_analyse(num, total, sorted_daily_trans_df)
    # basic_analyse(num, total, sorted_daily_trans_df)
    return basic_analyse(num, total, sorted_daily_trans_df, pro, ts_code)


def run():
    # 1. 初始化pro接口
    pro = ts.pro_api(TOKEN)

    # 2. 拉取所有股票数据, 并返回根据基本信息筛选出满足条件的股票
    stock_list_df = get_satisfied_stocks(pro)
    stock_list_num = stock_list_df.shape[0]
    print("===> 满足要求的 A 股主板、中小盘上市公司总数为: ", stock_list_num)

    # 3. 拉取每只股票的详细数据并进行分析
    future_list = []
    res_df_list = []
    counter = 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for index, row in stock_list_df.iterrows():
            ts_code = row['ts_code']
            future = executor.submit(get_daily_trans_and_analyse, counter, stock_list_num, pro, ts_code)
            future_list.append(future)
            print("  ===> 提交: ", counter, "/", stock_list_num, "支股票进行分析")
            counter += 1

    for i in range(len(future_list)):
        res = future_list[i].result(timeout=THREAD_TIMEOUT_IN_SEC)
        res_df_list.append(res)
        print("  ===> 已获取到: ", i + 1, "/", stock_list_num, "支股票分析结果 ", len(res), "条")

    # 4. 合并所有结果
    # all_res_df = pd.DataFrame(columns=['ts_code', 'trade_date', 'pct_change', 'close', 'change', 'open', 'high', 'low',
    #                                    'pre_close', 'vol_ratio', 'turn_over', 'vol', 'amount', "total_share",
    #                                    "float_share", 'launch_date', 'grow_up_days'])
    all_res_df = pd.DataFrame(columns=['ts_code', 'trade_date', 'pct_change', 'close', 'change', 'open', 'high', 'low',
                                       'pre_close', 'vol_ratio', 'turn_over', 'vol', 'amount', "total_share",
                                       "float_share", 'highest_10_days', 'highest_10_days_pct', 'lowest_10_days',
                                       'lowest_10_days_pct', 'winner_rate'])
    for res_df in res_df_list:
        if len(res_df) > 0:
            all_res_df = pd.concat([all_res_df, res_df], ignore_index=True)

    all_res_df.to_excel('stock_limit_up_log.xlsx', sheet_name='stock_limit_up_log')


# 获取分钟级交易数据
def run2():
    import baostock as bs

    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond error_msg:' + lg.error_msg)

    #### 获取沪深A股历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
    # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
    # 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
    rs = bs.query_history_k_data_plus("sz.128074",
                                      "date,time,code,open,high,low,close,volume,amount,adjustflag",
                                      start_date='2017-07-01', end_date='2017-07-31',
                                      frequency="5", adjustflag="3")
    print('query_history_k_data_plus respond error_code:' + rs.error_code)
    print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)

    #### 结果集输出到csv文件 ####
    result.to_csv("D:\\history_A_stock_k_data.csv", index=False)
    print(result)

    #### 登出系统 ####
    bs.logout()


# 获取筹码分布
def run3():
    # 导入tushare
    # import tushare as ts
    # 初始化pro接口
    pro = ts.pro_api('8a5af498224fb2ebea8a11345fb4cfc81242631f66c7eebce8cdc055')

    # 拉取数据
    df = pro.cyq_chips(**{
        "ts_code": "600000.SH",
        "trade_date": "",
        "start_date": "",
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "price",
        "percent"
    ])
    print(df)


def test():
    pro = ts.pro_api(TOKEN)
    df = pro.cyq_perf(ts_code='600000.SH', start_date='20230104', end_date='20230504')

    print(df.columns)
    print(df)
    print(df['winner_rate'].iloc[0:9])
    print(type(df['winner_rate'].iloc[0:9].max().item()))


if __name__ == '__main__':
    run()
    # run2()
    # test()
