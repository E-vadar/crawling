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
        "trdDataApi": "QUALIFICATION_INFO",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "id": "",
            "keywor": "",
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
        return [i.get("businessId") for i in json.loads(state.get('data').get('result'))]
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def requestsData(business_ids, data_list_dic, company_name, companyid, myWriter):
    for i in business_ids:
        data = {
            "trdDataApi": "QUALIFICATION_DETAIL",
            "trdDataProvider": "TIANYANCHA",
            "trdDataRequest":{
                "businessId": i
            },
            "companyId": "",
            "companyName": company_name,
            "version": ""}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i,company_name)
        # print('state', state)
        parse(state, data_list_dic, company_name, companyid,myWriter)
        # return state

def parse(state,data_list_dic,company_name, companyid, myWriter):
    try:
        result = json.loads(state.get('data').get('result'))
        print(result)
        if (len(result)) != 0:
            myWriter.writerow(
                [companyid,
                 company_name,
                 do_time,
                 result.get("qualification").get('organ'),
                 result.get("qualification").get('effectiveTime'),
                 result.get("qualification").get('issuingCertificateTime'),
                 result.get("qualification").get('type'),
                 result.get("qualification").get('certificateNum'),
                 result.get("qualification").get('qualificationName').replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', ''),
                 result.get("certificate").get('validityPeriod'),
                 result.get("certificate").get('certNo'),
                 result.get("certificate").get('companyName'),
                 result.get("certificate").get('certDate'),
                 result.get("certificate").get('qualificationRange').replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', '')
                 ])
    except:
        traceback.print_exc()
        print(com_name)

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/qualification_detail.csv', 'w', encoding='utf-8', newline='') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            # company_name = '天地科技股份有限公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            business_ids = requestsPage(page_i, company_name)
            if business_ids:
                requestsData(business_ids, data_list_dic, company_name, companyid, myWriter)
            # break
