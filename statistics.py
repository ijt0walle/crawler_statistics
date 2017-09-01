# coding=utf-8

"""
统计各个站点的入库数量
"""

import datetime
import json
import time

import pandas as pd
import pymongo

from config import MONGO_CONFIG, CHECK_DATES
from logger import Logger

log = Logger("statistics.log").get_logger()

# mongodb 初始化
client = pymongo.MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'])
mongo_db = client[MONGO_CONFIG['db']]
mongo_db.authenticate(MONGO_CONFIG['username'], MONGO_CONFIG['password'])

"""
获取当天的delta天之前的日期
"""


def get_delta_date(delta):
    date_obj = datetime.datetime(int(time.strftime("%Y")), int(time.strftime("%m")), int(time.strftime("%d"))).date()
    diff = datetime.timedelta(days=delta)
    before_date = date_obj - diff
    return before_date.strftime("%Y-%m-%d")


# 迭代1和迭代2的主题
table_name_list1 = ["enterprise_data_gov", "enterprise_owing_tax", "penalty", "patent", "baidu_news",
                    "news", "ssgs_notice_cninfo", "ssgs_baseinfo", "ssgs_caibao_companies_ability",
                    "ssgs_caibao_assets_liabilities",
                    "ssgs_caibao_profit", "judgement_wenshu", "zhixing_info", "shixin_info", "judge_process",
                    "bid_detail", "bulletin", "court_ktgg"]
topic_name_list1 = [u"工商信息、变更信息", u"欠税信息", u"行政处罚", u"专利信息", u"百度新闻",
                    u"新闻", u"上市公告", u"上市公司基本信息表", u"上市公司财报-公司综合能力指标", u"上市公司财报-资产负债表",
                    u"上市公司财报-利润表", u"裁判文书", u"执行信息", u"失信信息", u"审判流程",
                    u"招中标信息", u"法院公告", u"开庭公告"]
table_name_list2 = ["ppp_project", "net_loan_blacklist", "investment_institutions", "investment_funds",
                    "financing_events",
                    "investment_events", "exit_event", "acquirer_event", "listing_events", "land_project_selling",
                    "loupan_lianjia", "ershoufang_lianjia", "xiaoqu_lianjia", "land_selling_auction", "land_auction"]
topic_name_list2 = [u"PPP项目库", u"网贷黑名单", u"投资机构", u"投资基金", u"投资基金-融资事件",
                    u"投资基金-投资事件", u"投资基金-退出事件", u"投资基金-并购事件", u"投资基金-上市事件", u"土地转让",
                    u"房地产-新房（链家）深圳市", u"房地产-二手在售房源深圳市", u"房地产-小区（链家）深圳市", u"土地基本信息", u"土地招拍挂"]

sheet_one_col_list = [u"主题", u"站点"]
sheet_two_col_list = [u"主题"]

# 合并
table_name_list = table_name_list1
table_name_list.extend(table_name_list2)
topic_name_list = topic_name_list1
topic_name_list.extend(topic_name_list2)


def main():
    start_date = get_delta_date(CHECK_DATES)
    end_date = time.strftime("%Y-%m-%d")

    start_time = start_date + " 00:00:00"
    end_time = end_date + " 23:59:59"

    sheet_one_col_list.append(start_time + u"至" + end_time)
    sheet_two_col_list.append(start_time + u"至" + end_time)

    sheet_one_list = []
    sheet_two_list = []

    log.info("开始启动统计..")
    log.info("当前统计的时间段为: {} - {}".format(start_time, end_time))
    for index, table_name in enumerate(table_name_list):

        count = 0
        log.info("当前统计的topic为: {}".format(table_name))

        collection = mongo_db[table_name]
        cursor = collection.find({'_utime': {'$gte': start_time, '$lte': end_time}},
                                 ['_src'],
                                 no_cursor_timeout=True).batch_size(1000)
        # 站点与统计量的映射
        site_count_map = {}
        for item in cursor:
            count += 1
            if '_src' in item and len(item["_src"]) > 0 and "site" in item["_src"][0]:
                cur_site = item["_src"][0]["site"].strip()
                site_count_map[cur_site] = site_count_map[cur_site] + 1 if cur_site in site_count_map else 1
            else:
                _id = item.pop('_id')
                log.warn("当前数据_src不符合条件:  {} {} {}".format(
                    topic_name_list[index] + table_name, _id,
                    json.dumps(item, ensure_ascii=False)))
            if count % 1000 == 0:
                log.info("当前进度: {} {}".format(table_name, count))

        log.info("总数据量: {} {}".format(table_name, count))
        cursor.close()

        total_count = 0
        sort_count_list = sorted(site_count_map.items(), key=lambda it: it[0])
        for _site, site_count in sort_count_list:
            total_count += site_count
            item = {u"主题": topic_name_list[index] + table_name,
                    u"站点": _site,
                    sheet_one_col_list[-1]: site_count}
            log.info(json.dumps(item, ensure_ascii=False))
            sheet_one_list.append(item)

        # 计算总量
        total_item = {u"主题": topic_name_list[index] + table_name,
                      sheet_two_col_list[-1]: total_count}
        sheet_two_list.append(total_item)
        log.info(json.dumps(total_item, ensure_ascii=False))

    df = pd.DataFrame(sheet_one_list, columns=sheet_one_col_list)
    df2 = pd.DataFrame(sheet_two_list, columns=sheet_two_col_list)
    with pd.ExcelWriter("{st}_{et}_utime_sites_statistics.xls".format(st=start_date, et=end_date)) as writer:
        df.to_excel(writer, index=False)
        df2.to_excel(writer, sheet_name="sheet2", index=False)
    log.info('统计结束...')


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        log.error("程序异常退出:")
        log.exception(ex)
