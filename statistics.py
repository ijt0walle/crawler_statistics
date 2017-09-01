# coding=utf-8

"""
统计各个站点的入库数量
"""

import datetime
import json
import re
import time

import MySQLdb
import pandas as pd
import pymongo

from config import CHECK_TOPIC, MONGO_CONFIG, MYSQL_CONFIG, CHECK_DATES
from logger import Logger

log = Logger("statistics.log").get_logger()

# mongodb 初始化
client = pymongo.MongoClient(MONGO_CONFIG['host'], MONGO_CONFIG['port'])
mongo_db = client[MONGO_CONFIG['db']]
mongo_db.authenticate(MONGO_CONFIG['username'], MONGO_CONFIG['password'])

# mysql 初始化
mysql_db = MySQLdb.connect(MYSQL_CONFIG['host'],
                           MYSQL_CONFIG['username'],
                           MYSQL_CONFIG['password'],
                           MYSQL_CONFIG['db'], charset="utf8")

"""
验证日期格式是否是形如xxxx-xx-xx的形式
如果不是，则抛出异常
"""


def check_date_formate(date):
    p = re.compile("\d{4}-\d{2}-\d{2}")
    m = p.match(date)
    if m is None:
        raise Exception("the formate of date is error! correct formate is: xxxx-xx-xx")


"""
获取当天的delta天之前的日期
"""


def get_delta_date(delta):
    date_obj = datetime.datetime(int(time.strftime("%Y")), int(time.strftime("%m")), int(time.strftime("%d"))).date()
    diff = datetime.timedelta(days=delta)
    before_date = date_obj - diff
    return before_date.strftime("%Y-%m-%d")


"""
获取date前一天的日期
"""


def get_yesterday(date):
    date = date.split(" ")[0]
    time_obj = time.strptime(date, "%Y-%m-%d")
    date_obj = datetime.datetime(time_obj[0], time_obj[1], time_obj[2]).date()
    one_day = datetime.timedelta(days=1)
    yesterday = date_obj - one_day
    return yesterday.strftime("%Y-%m-%d")


# 根据topic获取topic_id
def get_topic_id(topic):
    topic_id = 0

    cursor = mysql_db.cursor()
    sql = "SELECT * FROM topic WHERE table_name = '%s' " % (topic)
    try:
        cursor.execute(sql)
        one_topic = cursor.fetchone()
        topic_id = one_topic[0]
    except Exception as e:
        log.error("Error: unable to fecth data")
        log.exception(e)

    cursor.close()
    return topic_id


# 根据topic_id获取主题的所有站点
def get_sites_by_topic_id(topic_id):
    res = []
    cursor = mysql_db.cursor()
    sql = "SELECT * FROM site"
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        # 遍历sites
        for row in rows:
            label = row[10]
            site = row[7]
            topic_ids = label.split(",") if label else []
            if str(topic_id) in topic_ids:
                res.append(site)
    except Exception as e:
        log.error("Error: unable to fecth data")
        log.exception(e)
    cursor.close()
    return res


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

cols = [u"主题", u"站点"]
cols2 = [u"主题"]

# 合并
table_name_list = table_name_list1
table_name_list.extend(table_name_list2)
topic_name_list = topic_name_list1
topic_name_list.extend(topic_name_list2)


# @click.command()
# @click.option("-s", "--st", default="", help=u"统计的开始时间")
# @click.option("-e", "--et", default="", help=u"统计的结束时间")
def main():
    # 只要有一个日期为""，则st为当前日期的七天之前的日期，et为当天日期

    start_date = get_delta_date(CHECK_DATES)
    end_date = time.strftime("%Y-%m-%d")

    start_time = start_date + " 00:00:00"
    end_time = end_date + " 23:59:59"

    check_date_formate(start_time)
    check_date_formate(end_time)
    cols.append(start_time + u"至" + end_time)
    cols.append(u"TAG")
    cols2.append(start_time + u"至" + end_time)

    sheet_one_list = []
    sheet_two_list = []

    log.info("开始启动统计..")
    log.info("当前统计的时间段为: {} - {}".format(start_time, end_time))
    for index, table_name in enumerate(table_name_list):

        count = 0
        log.info("当前统计的topic为: {}".format(table_name))

        collection = mongo_db[table_name]
        if table_name in CHECK_TOPIC:
            site_list = CHECK_TOPIC[table_name]["sites"]
        else:
            site_list = []
            sites_str_list = get_sites_by_topic_id(get_topic_id(table_name))
            for ss in sites_str_list:
                site_list.append({"site": ss})

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

        # maps.append(site_count_map)
        cursor.close()

        for site_item in site_list:
            site_info = site_item['site']
            site_tmp = {u"主题": topic_name_list[index] + table_name, u"站点": site_info, u"TAG": ""}

            site_num = site_count_map.get(site_info)
            if site_num is not None:
                site_tmp[cols[2]] = site_num
                continue

            site_tmp[cols[2]] = -1
            for key, value in site_count_map.items():
                if key in site_info or site_info in key:
                    site_tmp[cols[2]] = value
                    log.info('in 操作找到站点信息: {} {}'.format(topic_name_list[index] + table_name, site_info))
                    break

            if site_tmp[cols[2]] == -1:
                site_tmp[cols[2]] = 0
                log.warn('当前站点没有找到数据信息: {} {}'.format(topic_name_list[index] + table_name, site_info))

            sheet_one_list.append(site_tmp)

        # 计算总量
        sheet_two_tmp = {u"主题": topic_name_list[index] + table_name}
        count = 0

        for site_item in site_list:
            count += site_count_map.get(site_item['site'], 0)
        sheet_two_tmp[cols2[1]] = count
        sheet_two_list.append(sheet_two_tmp)

    df = pd.DataFrame(sheet_one_list, columns=cols)
    df2 = pd.DataFrame(sheet_two_list, columns=cols2)
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
