"""
973
现金流
"""

import requests
import json
import math
import sys
import csv
sys.path.append('D:/qiyuanwork/pythonProject/parse')
from lib import get_con
import time
import uuid
from datetime import datetime
import requests
import json
import math
import sys
import csv
sys.path.append('D:/qiyuanwork/pythonProject/parse')
from lib import get_con
import time
import uuid
from datetime import datetime

filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
do_time = datetime.now().strftime("%Y-%m-%d")


def requestsPage(i, company_name):
    data = {
        "trdDataApi":"CASHFLOW_INFO",
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
        # print(page)
        years = [i.get("value") for i in page]
        # print('get data num', page)
        return years
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def string_2_num(money):
    if isinstance(money, str):
        if money and money.find("亿") != -1:
            return int(float(money[:money.find("亿")]) * 100000000)
        elif money and money.find("万") != -1:
            return int(float(money[:money.find("万")]) * 10000)
        else:
            return money
    else:
        return money

def requestsData(years,data_list_dic,company_name,companyid,myWriter):
    # print(years)
    for i in years:
        data = {"trdDataApi": "CASHFLOW_INFO", "trdDataProvider": "TIANYANCHA", "trdDataRequest":
            {"id": "", "year": i, "name": company_name,},
                "companyId": "", "companyName": company_name, "version": ""}
        # pageSize = 10
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        parse(state,data_list_dic,company_name,companyid,myWriter)

def parse(state,data_list_dic,company_name, companyid, myWriter):
    state_data = state
    data_dic = {}
    try:
        result = json.loads(state.get('data').get('result'))

        data_dic['corpCashFlow'] = result.get('corpCashFlow')
        if (len(data_dic['corpCashFlow'])) != 0:
            for j in range(len(data_dic['corpCashFlow'])):
                myWriter.writerow(
                          [companyid,
                          company_name,
                          do_time,
                          string_2_num(data_dic['corpCashFlow'][j].get('payments_of_all_taxes')),
                          string_2_num(data_dic['corpCashFlow'][j].get('other_cash_paid_related_to_oa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('sub_total_of_ci_from_ia')),
                          string_2_num(data_dic['corpCashFlow'][j].get('showYear')),
                          string_2_num(data_dic['corpCashFlow'][j].get('sub_total_of_cos_from_oa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('invest_paid_cash')),
                          string_2_num(data_dic['corpCashFlow'][j].get('ncf_from_oa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('sub_total_of_cos_from_ia')),
                          string_2_num(data_dic['corpCashFlow'][j].get('initial_balance_of_cce')),
                          string_2_num(data_dic['corpCashFlow'][j].get('ncf_from_ia')),
                          string_2_num(data_dic['corpCashFlow'][j].get('net_increase_in_cce')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_paid_for_assets')),
                          string_2_num(data_dic['corpCashFlow'][j].get('final_balance_of_cce')),
                          string_2_num(data_dic['corpCashFlow'][j].get('ncf_from_fa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_received_of_other_fa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('goods_buy_and_service_cash_pay')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_received_of_dspsl_invest')),
                          string_2_num(data_dic['corpCashFlow'][j].get('other_cash_paid_relating_to_fa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('sub_total_of_ci_from_fa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('sub_total_of_cos_from_fa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_received_of_borrowing')),
                          string_2_num(data_dic['corpCashFlow'][j].get('invest_income_cash_received')),
                          string_2_num(data_dic['corpCashFlow'][j].get('net_cash_of_disposal_assets')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_paid_to_staff_etc')),
                          string_2_num(data_dic['corpCashFlow'][j].get('effect_of_exchange_chg_on_cce')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_pay_for_debt')),
                          string_2_num(data_dic['corpCashFlow'][j].get('sub_total_of_ci_from_oa')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_received_of_sales_service')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_received_of_other_ia')),
                          string_2_num(data_dic['corpCashFlow'][j].get('cash_paid_of_distribution'))
                           ])
            data_list_dic.clear()
    except:
        print(com_name)

if __name__ == '__main__':

    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/cashflow.csv', 'w', encoding='utf-8', newline = '') as myFile:
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

