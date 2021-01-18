

"""
文件说明：
进出口信用

描述：
可以通过公司名称或ID获取企业进出口信用信息，企业进出口信用信息包括海关注册编码、注册海关、经营类别等字段的详细信息

接口ID：881
"""

"""

"""
import traceback
import requests
import json
import sys
import csv
import math
sys.path.append('D:/qiyuanwork/pythonProject/parse')
from lib import get_con
import uuid
from datetime import datetime

filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d")

def requestsPage(i, company_name):
    data = {
        "trdDataApi": "IMPORTANDEXPORY_INFO",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
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
    # print('state_one', state)
    if state.get('data').get('successFlag') == True:
        page = state.get('data').get('page').get('total')
        return page
    else:
        print('此公司无法获取数据,报错 %s' % (state.get('data').get('code')))
        return False

def requestsData(page, data_list_dic, company_name, companyid, myWriter1,myWriter2):
    page = math.ceil(page / 20)
    for i in range(page):
        data = {
            "trdDataApi": "IMPORTANDEXPORY_INFO",
            "trdDataProvider": "TIANYANCHA",
            "trdDataRequest":{
                "pageNum": i,
                "pageSize": 20,
                "name": company_name},
            "companyId": "",
            "companyName": company_name,
            "version": ""}
        # pageSize = 10
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        parse(state, data_list_dic, company_name, companyid,myWriter1,myWriter2)

def parse(state,data_list_dic,company_name, companyid, myWriter1, myWriter2):
    try:
        result = json.loads(state.get('data').get('result'))
        sanction =result.get("sanction")
        baseInfo =result.get("baseInfo")
        creditRating =result.get("creditRating")
        if sanction:
            for i in sanction:
                myWriter1.writerow(
                    [companyid,
                     company_name,
                     do_time,
                     baseInfo.get('industryCategory'),
                     baseInfo.get('annualReport'),
                     baseInfo.get('validityDate'),
                     baseInfo.get('status'),
                     baseInfo.get('economicDivision'),
                     baseInfo.get('managementCategory'),
                     baseInfo.get('administrativeDivision'),
                     baseInfo.get('recordDate'),
                     baseInfo.get('crCode'),
                     baseInfo.get('specialTradeArea'),
                     baseInfo.get('customsRegisteredAddress'),
                     baseInfo.get('types'),
                     i.get("penaltyDate"),
                     i.get("decisionNumber"),
                     i.get("party"),
                     i.get("natureOfCase")
                     ])

        if creditRating:
            for i in creditRating:
                myWriter2.writerow(
                    [companyid,
                     company_name,
                     do_time,
                    i.get('creditRating'),
                    i.get('authenticationCode'),
                    i.get('identificationTime')
             ])
    except:
        traceback.print_exc()
        print(com_name)

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/import_and_export_sanction_pre.csv', 'w', encoding='utf-8', newline='') as myFile1, \
            open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/import_and_export_credit_rating_pre.csv', 'w',
                 encoding='utf-8', newline='') as myFile2:
        myWriter1 = csv.writer(myFile1)
        myWriter2 = csv.writer(myFile2)

        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            # company_name = '全维度测试有限责任公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            page = requestsPage(page_i, company_name)
            if page:
                requestsData(page,
                             data_list_dic,
                             company_name,
                             companyid,
                             myWriter1,
                             myWriter2)
            # break
