"""
文件说明：
产品信息
被执行人
"""
import csv
import requests
import json
import math
import pandas as pd
import sys
sys.path.append('C:/Users/cong/PycharmProjects/pythonProject/parse')
from lib import get_con
import uuid
from conf import get_config
from datetime import datetime

filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d")

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

def requestsPage(company_name, companyid, myWriter):
    # ANNOUNCEMENTHISTORY_INFO
    data = {
        "trdDataApi": "ISSUE_RELATED",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "name": company_name},
        "companyId": "",
        "companyName": company_name,
        "version": ""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url='http://192.168.88.103:8901/trddata/getData', headers=headers, data=json.dumps(data))
    state=json.loads(response.text)
    # print('state_one',state)
    if state.get('data').get('successFlag'):
        try:
            result = json.loads(state.get('data').get('result'))
            myWriter.writerow([
                companyid,
                company_name,
                do_time,
                result.get("expectedToRaise"),
                result.get("mainUnderwriter").get("id"),
                result.get("mainUnderwriter").get("cType"),
                result.get("mainUnderwriter").get("name"),
                result.get("history").replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', ''),
                result.get("issueNumber"),
                result.get("issuePrice"),
                result.get("listingSponsor").get("id"),
                result.get("listingSponsor").get("cType"),
                result.get("listingSponsor").get("name"),
                result.get("ipoRatio"),
                result.get("rate"),
                result.get("listingDate"),
                result.get("issueDate"),
                result.get("openingPrice"),
            ])
        except:
            print("fail!")
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

if __name__ == '__main__':

    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数',len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_listed/issuerelated.csv', 'w', encoding='utf-8', newline='') as myFile:
        myWriter = csv.writer(myFile)
        count = 0
        for com_name in range(len(data_list)):
            company_name = data_list[com_name][1]
            print('获取公司名字', company_name)
            companyid = data_list[com_name][2]
            requestsPage(company_name, companyid, myWriter)