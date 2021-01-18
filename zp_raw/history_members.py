
"""
989
会员信息
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
        "trdDataApi": "HI_MEMBERS",
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
            # print(result)
            for position in ["pastLegalPersonList",  "pastholderList"]:
                items = result.get(position)
                if items:
                    for i in items:
                        myWriter.writerow([
                            companyid,
                            company_name,
                            do_time,
                            i.get("id"),
                            i.get("time"),
                            i.get("name")
                ])
            pastStafferList = result.get("pastStafferList")
            if pastStafferList:
                for i in pastStafferList:
                    for item in i:
                        myWriter.writerow([
                            companyid,
                            company_name,
                            do_time,
                            item.get("id"),
                            item.get("time"),
                            item.get("name")
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
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/history_members.csv', 'w', encoding='utf-8', newline = '') as myFile:
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
