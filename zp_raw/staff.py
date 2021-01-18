
"""
//接口ID：820
//接口名称：主要人员
//接口类型：数据获取
"""

import traceback
import sys
sys.path.append('D:/qiyuanwork/pythonProject/parse')

import requests
import json
import math
import sys
import csv
sys.path.append('D:/qiyuanwork/pythonProject/parse')
from lib import get_con

import uuid
from datetime import datetime

filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d")

def requestsPage(i, company_name):
    data = {
        "trdDataApi": "STAFF_INFO",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "keywor": company_name,
            "name": company_name,
            "pageNum": i,
            'pageSize': '20'},
        "companyId": "",
        "companyName": company_name,
        "version": ""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers, data=json.dumps(data))
    # print(response.text)
    state=json.loads(response.text)
    # print('state_one',state)
    if state.get('data').get('successFlag') == True:
        page = state.get('data').get('page').get('total')
        return page
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def requestsData(page, data_list_dic, company_name, companyid, myWriter):
    page = math.ceil(page / 20)
    for i in range(page):
        data = {
            "trdDataApi": "STAFF_INFO",
            "trdDataProvider": "TIANYANCHA",
            "trdDataRequest": {
                "name": company_name,
                "pageNum": i + 1,
                'pageSize': '20'},
            "companyId": "",
            "companyName": company_name,
            "version": ""}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i,company_name)
        # print('state', state)
        #
        parse(state,data_list_dic,company_name,companyid,myWriter)

def parse(state,data_list_dic,company_name, companyid, myWriter):
    try:
        result = json.loads(state.get('data').get('result'))
        if (result) != 0:
            for j in range(len(result)):
                # print(result[j])
                myWriter.writerow(
                          [companyid,
                          company_name,
                          do_time,
                         result[j].get('id'),
                         "".join(result[j].get('typeJoin')).replace(",", "#"),
                         result[j].get('logo'),
                         result[j].get('type'),
                         result[j].get('name')
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
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/staff.csv', 'w', encoding='utf-8', newline= '') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            # company_name = '北京百度网讯科技有限公司'
            page = requestsPage(page_i, company_name)
            #
            if page != False:
                requestsData(page,data_list_dic,company_name,companyid, myWriter)
            # break
