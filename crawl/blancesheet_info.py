# author:cong
# contact: 172212595@qq.com
# datetime:2020/11/17 12:45
# software: PyCharm

"""
972
资产负债表

"""

import requests
import traceback
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
do_time = datetime.now().strftime("%Y-%m-%d")


def requestsPage(i, company_name):
    data = {
        "trdDataApi":"BALANCESHEET_INFO",
        "trdDataProvider":"TIANYANCHA",
        "trdDataRequest":{"id":"",
                            "keywor":"",
                          "name":company_name,
                          "pageNum":i,
                          'pageSize':'20'},
        "companyId":"",
        "companyName":company_name,
        "version":""}
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
        data = {"trdDataApi": "BALANCESHEET_INFO", "trdDataProvider": "TIANYANCHA", "trdDataRequest":
            {"id": "", "year": i, "name": company_name,},
                "companyId": "", "companyName": company_name, "version": ""}
        # pageSize = 10
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i,company_name)
        # print('state', state)
        #
        parse(state,data_list_dic,company_name,companyid,myWriter)
        # return state

def string_2_num(money):
    if money and money.find("亿") != -1:
        return int(float(money[:money.find("亿")]) * 100000000)
    elif money and money.find("万") != -1:
        return int(float(money[:money.find("万")]) * 10000)
    else:
        return money

def parse(state,data_list_dic,company_name, companyid, myWriter):
    data_dic = {}
    try:
        result = json.loads(state.get('data').get('result'))

        data_dic['corpBalanceSheet'] = result.get('corpBalanceSheet')
        if (len(data_dic['corpBalanceSheet'])) != 0:
            for j in range(len(data_dic['corpBalanceSheet'])):
                myWriter.writerow(
                          [companyid,
                          company_name,
                          do_time,
                          string_2_num(data_dic['corpBalanceSheet'][j].get('tax_payable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('lt_deferred_expense')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_current_assets')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_assets')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('pre_payment')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('dividend_payable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('othr_noncurrent_assets')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('construction_in_process')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('bill_payable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_current_liab')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('showYear')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('inventory')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_quity_atsopc')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('intangible_assets')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('shares')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('othr_payables')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('undstrbtd_profit')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('payroll_payable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('dt_assets')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_liab_and_holders_equity')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('invest_property')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('fixed_asset')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('interest_payable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('account_receivable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('currency_funds')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('pre_receivable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_noncurrent_assets')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('othr_current_assets')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('earned_surplus')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_noncurrent_liab')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('bills_receivable')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('othr_receivables')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('capital_reserve')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_holders_equity')),
                          string_2_num(data_dic['corpBalanceSheet'][j].get('total_liab'))
                           ])
            data_list_dic.clear()
    except:
        traceback.print_exc()
        print("exception:",com_name)

if __name__ == '__main__':

    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file/balancesheet.csv', 'w', encoding='utf-8', newline = '') as myFile:
        myWriter = csv.writer(myFile)
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            print('company_name',company_name)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            years = requestsPage(page_i, company_name)
            # print(years)
            if years != False:
                requestsData(years,data_list_dic,company_name,companyid, myWriter)
            # break

