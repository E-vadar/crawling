
"""
1004
企业发展
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
        "trdDataApi": "CB_DEVELOPMENT",
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
            privateFundList = result.get("privateFundList")
            if privateFundList:
                for i in privateFundList:
                    myWriter1.writerow([
                        companyid,
                        company_name,
                        do_time,
                        i.get("reg_no"),
                        i.get("human_graph_id"),
                        i.get("org_type"),
                        i.get("company_graph_id"),
                        i.get("boss_name"),
                        i.get("manager_name"),
                        i.get("est_date"),
                        i.get("alias"),
                        i.get("logo")
                    ])
            jpList = result.get("jpList")
            if jpList:
                for i in jpList:
                    myWriter2.writerow([
                        companyid,
                        company_name,
                        do_time,
                        i.get("companyName"),
                        i.get("date"),
                        i.get("graphId"),
                        i.get("hangye"),
                        i.get("icon"),
                        i.get("iconOssPath"),
                        i.get("jingpinProduct"),
                        i.get("location"),
                        i.get("product"),
                        i.get("round"),
                        i.get("setupDate"),
                        i.get("value"),
                        i.get("yewu")
                    ])
            # print(result)
            tzList = result.get("tzList")
            if tzList:
                for i in tzList:
                    myWriter3.writerow([
                        companyid,
                        company_name,
                        do_time,
                        i.get("icon"),
                        i.get("location"),
                        i.get("yewu"),
                        i.get("hangye1"),
                        i.get("iconOssPath"),
                        i.get("tzdate"),
                        i.get("product"),
                        i.get("id"),
                        i.get("graph_id"),
                        i.get("company"),
                        i.get("money"),
                        i.get("lunci"),
                        i.get("rongzi_map").replace(",", "#") if i.get("rongzi_map") else "",
                        i.get("organization_name")
                    ])
            teamList = result.get("teamList")
            if teamList:
                for i in teamList:
                    myWriter4.writerow([
                        companyid,
                        company_name,
                        do_time,
                        i.get("companyName"),
                        i.get("desc").replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', ''),
                        i.get("graphId"),
                        i.get("hid"),
                        i.get("icon	"),
                        i.get("iconOssPath"),
                        i.get("isDimission"),
                        i.get("name"),
                        i.get("title")
                    ])
            investOrgList = result.get("investOrgList")
            if investOrgList:
                for i in investOrgList:
                    myWriter5.writerow([
                        companyid,
                        company_name,
                        do_time,
                        i.get("jigou_name"),
                        i.get("area"),
                        i.get("orgCode"),
                        i.get("imgPath"),
                        i.get("desc").replace('\n', '').replace(',', '#').replace('，', '#').replace('\r', ''),
                        i.get("foundYear")
                    ])
            rongziList = result.get("rongziList")
            if rongziList:
                for i in rongziList:
                    myWriter6.writerow([
                        companyid,
                        company_name,
                        do_time,
                        i.get("companyName"),
                        i.get("date"),
                        i.get("pubTime"),
                        i.get("investorName"),
                        i.get("money"),
                        i.get("newsTitle"),
                        i.get("newsUrl"),
                        i.get("round"),
                        i.get("share"),
                        i.get("value")
                    ])
            productList = result.get("productList")
            if productList:
                for i in productList:
                    myWriter7.writerow([
                        companyid,
                        company_name,
                        do_time,
                        i.get("companyName"),
                        i.get("graphId"),
                        i.get("hangye"),
                        i.get("logo"),
                        i.get("logoOssPath"),
                        i.get("product"),
                        i.get("setupDate"),
                        i.get("alias"),
                        i.get("yewu")
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
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/development_private_fund_list.csv', 'w',
              encoding='utf-8', newline='') as myFile1,\
          open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/development_competitor_list.csv', 'w',
               encoding='utf-8', newline='') as myFile2,\
          open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/development_invest_list.csv', 'w',
               encoding='utf-8', newline='') as myFile3,\
          open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/development_team_list.csv', 'w',
               encoding='utf-8', newline='') as myFile4,\
          open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/development_invest_org_list.csv', 'w',
               encoding='utf-8', newline='') as myFile5,\
          open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/development_finance_list.csv', 'w',
               encoding='utf-8', newline='') as myFile6,\
          open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/development_product_list.csv', 'w',
               encoding='utf-8', newline='') as myFile7:

        myWriter1 = csv.writer(myFile1)
        myWriter2 = csv.writer(myFile2)
        myWriter3 = csv.writer(myFile3)
        myWriter4 = csv.writer(myFile4)
        myWriter5 = csv.writer(myFile5)
        myWriter6 = csv.writer(myFile6)
        myWriter7 = csv.writer(myFile7)

        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]

            # company_name = '新疆中科援疆创新创业私募基金管理有限公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            requestsPage(page_i, company_name)
            # break
