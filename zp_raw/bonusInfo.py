# contact: 172212595@qq.com
# datetime:2020/11/17 12:45
# software: PyCharm

"""
//接口ID：863
//接口名称：分红情况
//接口类型：数据获取
//接口说明：可以通过公司名称或ID获取上市公司分红情况信息，分红情况信息包括董事会日期、股东大会日期、实施日期、分红方案说明、分红率等
//完整URL示例：http://open.api.tianyancha.com/services/open/stock/bonusInfo/2.0?id=485568977&name=深圳市振业（集团）股份有限公司&keyword=深圳市振业（集团）股份有限公司&pageNum=1&pageSize=10
"""
import traceback
import sys
sys.path.append('D:/qiyuanwork/pythonProject/parse')
import uuid
from datetime import datetime
filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        "trdDataApi": "BONUS_INFO",
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
        page = state.get('data').get('page').get('total')
        return page
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def requestsData(page, data_list_dic, company_name, companyid, myWriter):
    page = math.ceil(page / 20)
    for i in range(page):
        data = {
            "trdDataApi": "BONUS_INFO",
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
        # page,state = requestsPage(i,company_name)
        # print('state', state)
        parse(state, data_list_dic, company_name, companyid,myWriter)
        # return state

def parse(state,data_list_dic,company_name, companyid, myWriter):
    try:
        result = json.loads(state.get('data').get('result'))
        # print(result, "xxxxxxxxxxxxxxxxxxxxxxxxx")
        if (len(result)) != 0:
            for j in range(len(result)):
                # print(result[j])
                myWriter.writerow(
                          [companyid,
                          company_name,
                          do_time,
                          result[j].get('progress'),
                          result[j].get('shareholderDate'),
                          result[j].get('acuxiDate	'),
                          result[j].get('payment'),
                          result[j].get('implementationDate'),
                          result[j].get('asharesDate'),
                          result[j].get('boardDate'),
                          result[j].get('dividendRate'),
                          result[j].get('introduction')
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
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file/bonusInfo.csv', 'w', encoding='utf-8', newline='') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            # company_name = '北京久其软件股份有限公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            page = requestsPage(page_i, company_name)
            if page != False:
                requestsData(page, data_list_dic, company_name, companyid, myWriter)
            # break
