
"""
主要指标（年度）
"""
import requests
import traceback
import json
import math
import sys
import csv
sys.path.append('D:/qiyuanwork/pythonProject/parse')
from lib import get_con
import uuid
from datetime import datetime

filename = uuid.uuid4()
do_time = datetime.now().strftime("%Y-%m-%d")
fields = ["crfgsasr_to_revenue","np_atsopc_nrgal_yoy","asset_liab_ratio","revenue_yoy",
           "net_profit_atsopc_yoy","fully_dlt_roe","tax_rate","receivable_turnover_days","pre_receivable",
           "current_ratio","operate_cash_flow_ps","showYear","gross_selling_rate",
           "current_liab_to_total_liab","quick_ratio","net_interest_of_total_assets",
           "operating_total_revenue_lrr_sq","profit_deduct_nrgal_lrr_sq","wgt_avg_roe","basic_eps",
           "net_selling_rate","total_capital_turnover","net_profit_atsopc_lrr_sq","net_profit_per_share",
           "capital_reserve","profit_nrgal_sq","inventory_turnover_days","total_revenue",
           "undistri_profit_ps","dlt_earnings_per_share","net_profit_atsopc","basic_e_ps_net_of_nrgal"]

def requestsPage(i, company_name):
    data = {
        "trdDataApi":"INDICATORS_QUARTERLY_INFO",
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
    for i in years:
        data = {"trdDataApi": "INDICATORS_QUARTERLY_INFO", "trdDataProvider": "TIANYANCHA", "trdDataRequest":
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

def parse(state,data_list_dic,company_name, companyid, myWriter):
    state_data = state
    data_dic = {}
    try:
        result = json.loads(state.get('data').get('result'))
        data_dic['corpTotalMainIndex'] = result.get('corpTotalMainIndex')
        if (len(data_dic['corpTotalMainIndex'])) != 0:
            for j in range(len(data_dic['corpTotalMainIndex'])):
                line_index = [companyid,
                          company_name,
                          do_time]
                for index in fields:
                    line_index.append(
                        string_2_num(data_dic['corpTotalMainIndex'][j].get(index))
                    )

                myWriter.writerow(
                    line_index
                          )
            data_list_dic.clear()
    except:
        traceback.print_exc()
        print(com_name)

if __name__ == '__main__':

    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    # print('获取公司条数', data_list, len(data_list))
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/indicators_info.csv', 'w', encoding='utf-8', newline = '') as myFile:
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
