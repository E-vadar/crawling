"""
实际控制人
"""
import sys
sys.path.append('D:/qiyuanwork/pythonProject/parse')
import requests
import json
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
        "trdDataApi": "ACTUAL_CONTROL",
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
    print('state_one',state)
    if state.get('data').get('successFlag') == True:
        try:
            result = json.loads(state.get('data').get('result'))
            # print(result)
            actualController = result.get("actualController")
            myWriter.writerow([
                companyid,
                company_name,
                do_time,
                actualController.get("hId"),
                actualController.get("gId"),
                actualController.get("name"),
                actualController.get("type"),
            ])
        except:
            import traceback
            traceback.print_exc()
            print("fail!")
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/actual_control.csv', 'w', encoding='utf-8', newline='') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            # company_name = '北京百度网讯科技有限公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            requestsPage(page_i, company_name)
            # break
