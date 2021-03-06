
"""
//接口ID：994
//接口名称：诚信信息
//接口类型：数据获取
//接口说明：可以通过公司名称或ID获取相关私募基金诚信信息，包括机构信息最后更新时间、特别提示信息
//完整URL示例：http://open.api.tianyancha.com/services/open/pf/integrity/2.0?id=2322352210&name=新疆中科援疆创新创业私募基金管理有限公司&keyword=新疆中科援疆创新创业私募基金管理有限公司

"""
import requests
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
        "trdDataApi": "LEGAL_OPINION",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "name": company_name,
        },
        "companyId": "",
        "companyName": company_name,
        "version": ""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=' http://192.168.88.102:8901/trddata/getData', headers=headers, data=json.dumps(data))
    state=json.loads(response.text)
    print('state_one',state)
    if state.get('data').get('successFlag') == True:
        try:
            result = json.loads(state.get('data').get('result'))
            myWriter.writerow([
                companyid,
                company_name,
                do_time,
                result.get("graph_id"),
                result.get("legal_opinion_status")
            ])
        except:
            print("fail!")
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/legal_opinion.csv', 'w', encoding='utf-8', newline='') as myFile:
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
