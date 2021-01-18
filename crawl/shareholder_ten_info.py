"""
十大股东（十大流通股东）
描述：
可以通过公司名称或ID获取上市公司十大股东和十大流通股东信息，十大股东和十大流通股东信息包括机构或基金、持有数量、持股变化、占股本比例、实际增减持、股份类型等
"""

import requests
import json
import math
import sys
import csv
sys.path.append('D:/qiyuanwork/pythonProject/parse')
from lib import get_con
import time
import pyhdfs
import uuid
from datetime import datetime

filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d")

def requestsPage(i, company_name):
    data = {
        "trdDataApi":"SHAREHOLDER_INFO",
        "trdDataProvider":"TIANYANCHA",
        "trdDataRequest":{"id":"",
                            "time":"",
                          "name":company_name,
                          "pageNum":i,
                          'pageSize':'20'},
        "companyId":"",
        "companyName":company_name,
        "version":""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers, data=json.dumps(data))
    # print(response.text)
    state=json.loads(response.text)
    # print('state_one',state)

    # if i==0:
    if state.get('data').get('successFlag') == True:
        page = state.get('data').get('page').get('total')
        # print('get data num', page)
        return page
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def string_2_num(money):
    if isinstance(money, str):
        if money and money.find("亿") != -1:
            return int(float(money[:money.find("亿")]) * 100000000)
        elif money and money.find("万") != -1:
            return int(float(money[:money.find("万")]) * 10000)
        else:
            return money
    else:
        return money

def requestsData(page,data_list_dic,company_name,companyid,myWriter):
    # page = math.ceil(page/10)
    # print('根据条数计算页数',page)
    # print('test',math.ceil(page/5))
    # company_name = '深圳市振业（集团）股份有限公司'
    #注意！！！！！！
    if math.ceil(page/20) >= 5:
        page=5
    else:
        page=math.ceil(page/20)
    # print('最终爬取的页数',page)
    for i in range(page):
        i=i+1
        # print('page',i)
        data = {"trdDataApi": "SHAREHOLDER_INFO", "trdDataProvider": "TIANYANCHA", "trdDataRequest":
            {"id": "", "startTime": "20190101", "endTime": "29991116", "name": company_name,
             "pageNum": i},
                "companyId": "", "companyName": company_name, "version": ""}
        # pageSize = 10
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i)
        # print('state', state)
        #
        parse(state,data_list_dic,company_name,companyid,myWriter)
        # return state

def parse(state,data_list_dic,company_name, companyid, myWriter):
    state_data = state
    data_dic = {}
    try:
        state = json.loads(state.get('data').get('result'))
        # print('leng',len(state))
        # for i in range(len(state)):
        data_dic['uid'] = state_data.get('data').get('noSqlId')
        data_dic['total'] = state_data.get('data').get('page').get('total')
        data_dic['companyid'] = companyid
        data_dic['timeList'] = state.get('timeList')
        aa = '/'.join(data_dic['timeList'])
        data_dic['timeList'] = aa
        data_dic['holderList'] = state.get('holderList')
        if (len(data_dic['holderList'])) != 0:
            for j in range(len(data_dic['holderList'])):
                myWriter.writerow(
                          [companyid,
                          company_name,
                          do_time,
                          string_2_num(data_dic['holderList'][j].get('proportion')),
                          string_2_num(data_dic['holderList'][j].get('mtenPercent')),
                          string_2_num(data_dic['holderList'][j].get('graphId')),
                          string_2_num(data_dic['holderList'][j].get('tenPercent')),
                          string_2_num(data_dic['holderList'][j].get('mtenTotal')),
                          string_2_num(data_dic['holderList'][j].get('type')),
                          string_2_num(data_dic['holderList'][j].get('tenTotal')),
                          string_2_num(data_dic['holderList'][j].get('cType')),
                          string_2_num(data_dic['holderList'][j].get('id')),
                          string_2_num(data_dic['holderList'][j].get('name')),
                          string_2_num(data_dic['holderList'][j].get('actual')),
                          string_2_num(data_dic['holderList'][j].get('sorting')),
                          string_2_num(data_dic['holderList'][j].get('holdingChange')),
                          string_2_num(data_dic['holderList'][j].get('holdingNum')),
                          string_2_num(data_dic['holderList'][j].get('shareType')),
                          string_2_num(data_dic['holderList'][j].get('compareChange')),
                          string_2_num(data_dic['holderList'][j].get('publishDate'))
                           ])
            data_list_dic.clear()
    except:
        print(com_name)

if __name__ == '__main__':

    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/shareholds.csv', 'w', encoding='utf-8', newline = '') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            print('company_name',company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            page = requestsPage(page_i, company_name)

            if page != False:
                requestsData(page,data_list_dic,company_name,companyid, myWriter)

