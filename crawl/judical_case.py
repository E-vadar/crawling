import requests
import traceback
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
        "trdDataApi": "JUDICIAL_CASE",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "id": "",
            "keyword": "",
            "name": company_name,
            "pageNum": i,
            'pageSize': '20'},
        "companyId": "",
        "companyName": company_name,
        "version": ""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=' http://192.168.88.102:8901/trddata/getData', headers=headers, data=json.dumps(data))
    # print(response.text)
    state = json.loads(response.text)
    # print('state_one', state)

    if state.get('data').get('successFlag') == True:
        page = state.get('data').get('page').get('total')
        return page
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def requestsData(page, company_name, companyid, myWriter):
    page = math.ceil(page / 20)
    if page > 10:
        page = 10
    for i in range(page):
        data = {
            "trdDataApi": "JUDICIAL_CASE",
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
        response = requests.post(url=' http://192.168.88.101:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i,company_name)
        # print('state', state)
        parse(state, company_name, companyid,myWriter)
        # return state

def parse(state,company_name, companyid, myWriter):
    try:
        result = json.loads(state.get('data').get('result'))
        if (len(result)) != 0:
            for j in range(len(result)):
                caseIdentity = result[j].get("caseIdentity")
                if caseIdentity:
                    for i in caseIdentity:
                        myWriter.writerow(
                            [companyid,
                             company_name,
                             do_time,
                             result[j].get('caseCode'),
                             result[j].get('trialTime'),
                             i,
                             result[j].get('caseTitle').replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', ''),
                             result[j].get('uuid'),
                             result[j].get('trialProcedure').replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', ''),
                             result[j].get('caseReason').replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', ''),
                             result[j].get('caseType').replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', '')
                             ])
                else:
                    myWriter.writerow(
                              [companyid,
                              company_name,
                              do_time,
                              result[j].get('caseCode'),
                              result[j].get('trialTime'),
                              '',
                              result[j].get('caseTitle'),
                              result[j].get('uuid'),
                              result[j].get('trialProcedure'),
                              result[j].get('caseReason'),
                              result[j].get('caseType')
                               ])
    except:
        traceback.print_exc()
        print(com_name)

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/judical_case_listed.csv', 'w', encoding='utf-8', newline='') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            # company_name = '上海灿星文化传媒股份有限公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            page = requestsPage(page_i, company_name)
            if page != False:
                requestsData(page, company_name, companyid, myWriter)
            # break
