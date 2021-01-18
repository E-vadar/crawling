# contact: 172212595@qq.com
# datetime:2020/11/17 12:45
# software: PyCharm

"""
//接口ID：455
//接口名称：股权穿透
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

def requestsPage(i, company_name, companyid, myWriter):
    data = {
        "trdDataApi": "INVESTTREE_INFO",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "flag": 3,
            "dir": 'i',
            "name": company_name,
            },
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
        result = json.loads(state.get('data').get('result'))
        recurve(i,result,companyid, company_name,myWriter)
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))

def recurve(dir,children, company_id, company_name,myWriter):
    if not children:
        return
    else:
        for i in children:
            if i:
                myWriter.writerow(
                    [
                        company_id,
                        company_name,
                        do_time,
                        dir,
                        i.get("id"),
                        i.get("lable"),
                        i.get("name"),
                        i.get("open"),
                        i.get("percent",0),
                        # i.get("children")[0].get("name") if i.get("children") else ''
                    ])
                recurve(dir, i.get("children"), company_id, company_name, myWriter)

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/investtree.csv', 'w', encoding='utf-8', newline='') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            company_name = data_list[com_name][1]
            # company_name = '中航重机股份有限公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            for i in ('up','down'):
                requestsPage(dir, company_name, companyid, myWriter)
                # break
            # break
