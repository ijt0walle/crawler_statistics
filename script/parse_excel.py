#!/usr/bin/env python
# encoding: utf-8
"""
@author: youfeng
@email: youfeng243@163.com
@license: Apache Licence
@file: parse_excel.py
@time: 2017/9/2 11:15
"""

import pandas
import xlrd

from config import TOPIC_NAME_LIST, TABLE_NAME_LIST


def main():
    excel = xlrd.open_workbook('2017-08-21_2017-08-27.xls')
    sheet = excel.sheet_by_index(0)

    n_rows = sheet.nrows
    print n_rows

    n_cols = sheet.ncols
    print n_cols

    # 0:主题 1:站点 4:指派人员 6:官方统计总数 9: 备注
    # {'主题': [{  }]}
    data_dict = dict()
    topic_hash = dict()
    count = 0
    for index in xrange(1, n_rows):
        # 获得topic 名称
        topic_name = sheet.cell(index, 0).value

        # 获得站点
        site = sheet.cell(index, 1).value

        # 获得指派人员
        person = sheet.cell(index, 4).value

        # 官方统计总数
        total_count = sheet.cell(index, 6).value

        # 备注
        remark = sheet.cell(index, 9).value

        if topic_name not in topic_hash:
            topic_hash[topic_name] = set()
        if topic_name not in data_dict:
            data_dict[topic_name] = list()

        # 如果站点重复，则过滤掉
        if site in topic_hash[topic_name]:
            continue

        topic_hash[topic_name].add(site)
        data_dict[topic_name].append({
            u'主题': topic_name,
            u'站点': site,
            u'指派人员': person,
            u'官方统计总数': total_count,
            u'备注': remark,
        })
        count += 1

    write_list = list()
    for index, name in enumerate(TOPIC_NAME_LIST):
        topic_name = name + TABLE_NAME_LIST[index]

        if topic_name not in data_dict:
            print "当前主题不存在: {}".format(topic_name)
            continue

        temp_list = data_dict[topic_name]
        sort_list = sorted(temp_list, key=lambda item: item[u'站点'])
        write_list.extend(sort_list)

    # df = pd.DataFrame(sheet_one_list, columns=sheet_one_col_list)
    # df2 = pd.DataFrame(sheet_two_list, columns=sheet_two_col_list)

    sheet_col_list = [u"主题", u"站点", u"指派人员", u"官方统计总数", u"备注"]
    df = pandas.DataFrame(write_list, columns=sheet_col_list)
    with pandas.ExcelWriter("total.xls") as writer:
        df.to_excel(writer, index=False)

    print "数据总数目为: {}".format(count)


if __name__ == '__main__':
    main()
