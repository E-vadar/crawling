# author:cong
# contact: 172212595@qq.com
# datetime:2020/11/17 12:45
# software: PyCharm

"""
主要指标（年度）
"""

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
do_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def requestsPage(i, company_name):
    data = {
        "trdDataApi":"PROFIT_INFO",
        "trdDataProvider":"TIANYANCHA",
        "trdDataRequest":{"id":"",
                            "keywor":"",
                          "name":company_name,
                          "pageNum":i,
                          'pageSize':'20'},
        "companyId": "",
        "companyName":company_name,
        "version": ""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers, data=json.dumps(data))
    # print(response.text)
    state=json.loads(response.text)
    # print('state_one',state)

    # if i==0:
    if state.get('data').get('successFlag') == True:
        page = json.loads(state.get('data').get('result')).get('corpFinancialYears')
        years = [i.get("value") for i in page]
        # print('get data num', page)
        return years
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def string_2_num(money):
    if money and money.find("亿") != -1:
        return int(float(money[:money.find("亿")]) * 100000000)
    elif money and money.find("万") != -1:
        return int(float(money[:money.find("万")]) * 10000)
    else:
        return money

def requestsData(years,data_list_dic,company_name,companyid,myWriter):
    # page = math.ceil(page/10)
    # print('根据条数计算页数',page)
    # print('test',math.ceil(page/5))
    # company_name = '深圳市振业（集团）股份有限公司'
    #注意！！！！！！
    # if math.ceil(page/20) >= 5:
    #     page=5
    # else:
    #     page=math.ceil(page/20)
    # # print('最终爬取的页数',page)
    for i in years:
        data = {"trdDataApi": "PROFIT_INFO", "trdDataProvider": "TIANYANCHA", "trdDataRequest":
            {"id": "", "year": i, "name": company_name,},
                "companyId": "", "companyName": company_name, "version": ""}
        # pageSize = 10
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # print(state)
        parse(state,data_list_dic,company_name,companyid,myWriter)

def parse(state,data_list_dic,company_name, companyid, myWriter):
    data_dic = {}
    try:
        result = json.loads(state.get('data').get('result'))
        data_dic['corpProfit'] = result.get('corpProfit')
        if (len(data_dic['corpProfit'])) != 0:
            for j in range(len(data_dic['corpProfit'])):
                myWriter.writerow(
                          [companyid,
                          company_name,
                          do_time,
                          string_2_num(data_dic['corpProfit'][j].get('total_compre_income_atsopc')),
                          string_2_num(data_dic['corpProfit'][j].get('profit_deduct_nrgal_ly_sq')),
                          string_2_num(data_dic['corpProfit'][j].get('showYear')),
                          string_2_num(data_dic['corpProfit'][j].get('financing_expenses')),
                          string_2_num(data_dic['corpProfit'][j].get('revenue')),
                          string_2_num(data_dic['corpProfit'][j].get('profit_total_amt')),
                          string_2_num(data_dic['corpProfit'][j].get('operating_costs')),
                          string_2_num(data_dic['corpProfit'][j].get('sales_fee')),
                          string_2_num(data_dic['corpProfit'][j].get('manage_fee')),
                          string_2_num(data_dic['corpProfit'][j].get('op')),
                          string_2_num(data_dic['corpProfit'][j].get('asset_impairment_loss')),
                          string_2_num(data_dic['corpProfit'][j].get('total_compre_income')),
                          string_2_num(data_dic['corpProfit'][j].get('invest_income')),
                          string_2_num(data_dic['corpProfit'][j].get('rad_cost')),
                          string_2_num(data_dic['corpProfit'][j].get('operating_taxes_and_surcharge')),
                          string_2_num(data_dic['corpProfit'][j].get('basic_eps')),
                          string_2_num(data_dic['corpProfit'][j].get('income_tax_expenses')),
                          string_2_num(data_dic['corpProfit'][j].get('total_revenue')),
                          string_2_num(data_dic['corpProfit'][j].get('net_profit')),
                          string_2_num(data_dic['corpProfit'][j].get('net_profit_atsopc')),
                          string_2_num(data_dic['corpProfit'][j].get('operating_cost')),
                          string_2_num(data_dic['corpProfit'][j].get('non_operating_income')),
                          string_2_num(data_dic['corpProfit'][j].get('othr_income')),
                          string_2_num(data_dic['corpProfit'][j].get('non_operating_payout'))])
            data_list_dic.clear()
    except:
        print(com_name)

if __name__ == '__main__':

    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/profit_info_1.csv', 'w', encoding='utf-8', newline = '') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            print('company_name',company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            years = requestsPage(page_i, company_name)

            if years != False:
                requestsData(years,data_list_dic,company_name,companyid, myWriter)
            # break

