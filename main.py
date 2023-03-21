import concurrent.futures

import pandas as pd
import tushare as ts

TOKEN = '8a5af498224fb2ebea8a11345fb4cfc81242631f66c7eebce8cdc055'
START_DATE = 20130101
END_DATE = 20230320
MAX_WORKERS = 4
THREAD_TIMEOUT_IN_SEC = 600


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


def get_daily_trans_and_analyse(num, total, pro, ts_code):
    res_df = pd.DataFrame(columns=["ts_code",
                                   "trade_date",
                                   "open",
                                   "high",
                                   "low",
                                   "close",
                                   "pre_close",
                                   "change",
                                   "pct_chg",
                                   "vol",
                                   "amount"])
    daily_trans_df = pro.daily(**{
        "ts_code": ts_code,
        "trade_date": "",
        "start_date": START_DATE,
        "end_date": END_DATE,
        "offset": "",
        "limit": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    sorted_daily_trans_df = daily_trans_df.sort_values(by=['trade_date'], ascending=[True])
    window = []
    for index, row in sorted_daily_trans_df.iterrows():
        # 涨停条件: 昨日收盘价 * 1.1 == 今日收盘价四舍五入到分
        pre_close = row['pre_close']
        close = row['close']
        trade_date = row['trade_date']
        if round(pre_close * 1.1, 2) == round(close, 2):  # 当日涨停
            window.append(trade_date)
            if len(window) > 1:
                res_df.loc[len(res_df)] = row
        elif len(window) > 0:  # 当日不涨停则重置窗口
            window = []
    print("  ===> 完成第: ", num, "/", total, "支股票的分析")
    return res_df


def run():
    # 1. 初始化pro接口
    pro = ts.pro_api(TOKEN)

    # 2. 拉取所有股票数据, 并返回根据基本信息筛选出满足条件的股票
    stock_list_df = get_satisfied_stocks(pro)
    stock_list_num = stock_list_df.shape[0]
    print("===> A 股主板、中小盘上市公司总数为: ", stock_list_num)

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
            if counter >= 10:
                break

    for i in range(len(future_list)):
        res_df_list.append(future_list[i].result(timeout=THREAD_TIMEOUT_IN_SEC))
        print("  ===> 已获取到: ", i + 1, "/", stock_list_num, "支股票分析结果")

    # 4. 合并所有结果
    all_res_df = pd.DataFrame(columns=["ts_code",
                                       "trade_date",
                                       "open",
                                       "high",
                                       "low",
                                       "close",
                                       "pre_close",
                                       "change",
                                       "pct_chg",
                                       "vol",
                                       "amount"])
    for res_df in res_df_list:
        if len(res_df) > 0:
            all_res_df = pd.concat([all_res_df, res_df], ignore_index=True)

    all_res_df.to_excel('stock_limit_up_log.xlsx', sheet_name='stock_limit_up_log')


if __name__ == '__main__':
    run()
