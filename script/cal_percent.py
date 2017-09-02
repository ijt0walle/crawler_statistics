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


def main():
    excel = xlrd.open_workbook('2017-08-26_2017-09-01_utime_sites_statistics.xls')
    sheet = excel.sheet_by_index(2)

    n_rows = sheet.nrows
    print n_rows

    n_cols = sheet.ncols
    print n_cols

    write_list = list()
    sheet_col_list = [u"主题", u"站点", u"数据库统计", u"官方统计总数", u"指派人员", u"备注"]

    for index in xrange(1, n_rows):
        data_list = list()
        # 获得topic 名称
        topic_name = sheet.cell(index, 0).value
        data_list.append(topic_name)

        # 获得站点
        site = sheet.cell(index, 1).value
        data_list.append(site)

        # 数据库总数
        db_count = sheet.cell(index, 2).value
        data_list.append(db_count)

        # 官方统计总数
        total_count = sheet.cell(index, 3).value
        data_list.append(total_count)

        # 指派人员
        person = sheet.cell(index, 4).value
        data_list.append(person)

        # 备注
        remark = sheet.cell(index, 5).value
        data_list.append(remark)

        data_dict = dict(zip(sheet_col_list, data_list))

        data_dict[u'占比'] = u""
        if total_count != u'' and total_count != '':
            total_int = int(total_count)
            if total_int != 0:
                data_dict[u'占比'] = str(round(db_count * 100 / total_int, 2)) + u"%"

        write_list.append(data_dict)

    sheet_col_list = [u"主题", u"站点", u"数据库统计", u"官方统计总数", u"占比", u"指派人员", u"备注"]

    df = pandas.DataFrame(write_list, columns=sheet_col_list)
    with pandas.ExcelWriter("all_sites.xls") as writer:
        df.to_excel(writer, index=False)


if __name__ == '__main__':
    main()
