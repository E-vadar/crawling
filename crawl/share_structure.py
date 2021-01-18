
"""
//接口ID：861
//接口名称：股本结构
//接口类型：数据获取
//接口说明：可以通过公司名称或ID获取上市公司股本结构信息，股本结构信息包括总股本、A股总股本、流通A股、限售A股等
//完整URL示例：http://open.api.tianyancha.com/services/open/stock/shareStructure/2.0?id=485568977&name=深圳市振业（集团）股份有限公司&keyword=深圳市振业（集团）股份有限公司&time=2019-06-30
"""

import requests
import traceback
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
        "trdDataApi":"SHARE_STRUCTURE",
        "trdDataProvider":"TIANYANCHA",
        "trdDataRequest":{
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
    if state.get('data').get('successFlag') == True:
        times = json.loads(state.get('data').get('result')).get('timeList')
        # print('get data num', page)
        return times
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

def requestsData(times,data_list_dic,company_name,companyid,myWriter):
    # page = math.ceil(page/10)
    # print('根据条数计算页数',page)
    # print('test',math.ceil(page/5))
    # company_name = '深圳市振业（集团）股份有限公司'
    #注意！！！！！！
    # if math.ceil(page/20) >= 5:
    #     page=5
    # else:
    #     page=math.ceil(page/20)
    # print('最终爬取的页数',page)
    for i in times:
        data = {
            "trdDataApi": "SHARE_STRUCTURE",
            "trdDataProvider": "TIANYANCHA",
            "trdDataRequest": {
                "time": i,
                "name": company_name,
                },
            "companyId": "",
            "companyName": company_name,
            "version": ""}
        # pageSize = 10
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i)
        # print('state', state)
        parse(state,data_list_dic,company_name,companyid,myWriter,i)
        # return state

def parse(state,data_list_dic,company_name, companyid, myWriter, year):
    try:
        dataList = json.loads(state.get('data').get('result')).get('dataList')
        # print(dataList)
        # print('leng',len(state))
        if (len(dataList)) != 0:
            for j in range(len(dataList)):
                myWriter.writerow(
                          [companyid,
                          company_name,
                          do_time,
                          year,
                          dataList[j].get("pubDate"),
                          dataList[j].get("changeReason"),
                          dataList[j].get("shareAll"),
                          dataList[j].get("noLimitShare"),
                          dataList[j].get("limitShare"),
                          dataList[j].get("ashareAll"),
                           ])
            data_list_dic.clear()
    except:
        traceback.print_exc()
        print(com_name)

if __name__ == '__main__':

    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/share_structure.csv', 'w', encoding='utf-8', newline='') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            print('company_name',company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            times = requestsPage(page_i, company_name)
            #
            if times:
                requestsData(times,data_list_dic,company_name,companyid, myWriter)
            # break

