
"""
988
机构信息
"""
import requests
import traceback
import json
import math
import sys
import csv
sys.path.append('D:/qiyuanwork/pythonProject/parse')
import uuid
from datetime import datetime

filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        "trdDataApi": "ORG_INFO",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "name": company_name,
        },
        "companyId": "",
        "companyName": company_name,
        "version": ""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers, data=json.dumps(data))
    state=json.loads(response.text)
    # print('state_one',state)
    if state.get('data').get('successFlag') == True:
        try:
            result = json.loads(state.get('data').get('result'))
            myWriter.writerow([
                companyid,
                company_name,
                do_time,
                result.get("org_type"),
                result.get("est_date"),
                result.get("manager_name_en"),
                result.get("pay_capital_scale"),
                result.get("office_addr"),
                result.get("reg_date"),
                result.get("reg_addr"),
                result.get("reg_no"),
                result.get("org_website"),
                result.get("pay_capital"),
                result.get("org_no"),
                result.get("graph_id"),
                result.get("org_integrity_info"),
                result.get("manager_name"),
                result.get("company_nature"),
                result.get("reg_capital"),
                result.get("business_type"),
            ])
        except:
            print("fail!")
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

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/org_info.csv', 'w', encoding='utf-8', newline = '') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]

            # company_name = '新疆中科援疆创新创业私募基金管理有限公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            requestsPage(page_i, company_name)
            # break
