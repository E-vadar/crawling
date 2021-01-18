import requests
import traceback
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
        "trdDataApi": "ANNUALREPORT_INFO",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
            "id": "",
            "keyword": "",
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
    state = json.loads(response.text)
    # print('state_one', state)

    if state.get('data').get('successFlag') == True:
        page = state.get('data').get('page').get('total')
        return page
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def requestsData(page, company_name, companyid, myWriter1,
                 myWriter2,
                 myWriter3,
                 myWriter4,
                 myWriter5,
                 myWriter6,
                 myWriter7,
                 myWriter8
                 ):
    page = math.ceil(page / 20)
    for i in range(page):
        data = {
            "trdDataApi": "ANNUALREPORT_INFO",
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
        response = requests.post(url=' http://192.168.88.101:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i,company_name)
        # print('state', state)
        parse(state, company_name, companyid,
              myWriter1,
              myWriter2,
              myWriter3,
              myWriter4,
              myWriter5,
              myWriter6,
              myWriter7,
              myWriter8
              )
        # return state

def parse(state,company_name, companyid,
          myWriter1,
          myWriter2,
          myWriter3,
          myWriter4,
          myWriter5,
          myWriter6,
          myWriter7,
          myWriter8):
    try:
        result = json.loads(state.get('data').get('result'))
        # print(result)
        if result:
            for i in result:
                # print(i)
                baseInfo = i.get("baseInfo")
                changeRecordList = i.get("changeRecordList")
                equityChangeInfoList = i.get("equityChangeInfoList")
                outGuaranteeInfoList = i.get("outGuaranteeInfoList")
                outboundInvestmentList = i.get("outboundInvestmentList")
                shareholderList = i.get("shareholderList")
                webInfoList = i.get("webInfoList")
                reportSocialSecurityInfo = i.get("reportSocialSecurityInfo")
                myWriter1.writerow(
                    [
                        companyid,
                        company_name,
                        do_time,
                        baseInfo.get("reportYear"),
                        baseInfo.get("companyName"),
                        baseInfo.get("creditCode"),
                        baseInfo.get("regNumber"),
                        baseInfo.get("phoneNumber"),
                        baseInfo.get("postcode"),
                        baseInfo.get("postalAddress"),
                        baseInfo.get("email"),
                        baseInfo.get("manageState"),
                        baseInfo.get("employeeNum"),
                        baseInfo.get("operatorName"),
                        baseInfo.get("totalAssets"),
                        baseInfo.get("totalEquity"),
                        baseInfo.get("totalSales"),
                        baseInfo.get("totalProfit"),
                        baseInfo.get("primeBusProfit"),
                        baseInfo.get("retainedProfit"),
                        baseInfo.get("totalTax"),
                        baseInfo.get("totalLiability")
                    ]
                )
                if changeRecordList:
                    for change_record in changeRecordList:
                        myWriter2.writerow(
                            [
                                companyid,
                                company_name,
                                do_time,
                                change_record.get("reportYear"),
                                change_record.get("changeItem"),
                                change_record.get("contentBefore"),
                                change_record.get("contentAfter"),
                                change_record.get("changeTime")
                            ]
                        )
                if equityChangeInfoList:
                    for equity_change_info in equityChangeInfoList:
                        myWriter3.writerow(
                            [
                                companyid,
                                company_name,
                                do_time,
                                equity_change_info.get("reportYear"),
                                equity_change_info.get("investorName"),
                                equity_change_info.get("ratioBefore"),
                                equity_change_info.get("ratioAfter"),
                                equity_change_info.get("changeTime")
                            ]
                        )
                if outGuaranteeInfoList:
                    for out_guarantee_info_list in outGuaranteeInfoList:
                        myWriter4.writerow(
                            [
                                companyid,
                                company_name,
                                do_time,
                                out_guarantee_info_list.get("reportYear"),
                                out_guarantee_info_list.get("creditor"),
                                out_guarantee_info_list.get("obligor"),
                                out_guarantee_info_list.get("creditoType"),
                                out_guarantee_info_list.get("creditoAmount"),
                                out_guarantee_info_list.get("creditoTerm"),
                                out_guarantee_info_list.get("guaranteeTerm"),
                                out_guarantee_info_list.get("guaranteeWay"),
                                out_guarantee_info_list.get("guaranteeScope")
                            ]
                        )
                if outboundInvestmentList:
                    for out_bound_investment_list in outboundInvestmentList:
                        myWriter5.writerow(
                            [
                                companyid,
                                company_name,
                                do_time,
                                out_bound_investment_list.get("reportYear"),
                                out_bound_investment_list.get("outcompanyName"),
                                out_bound_investment_list.get("regNum"),
                                out_bound_investment_list.get("creditCode"),
                                out_bound_investment_list.get("type"),
                                out_bound_investment_list.get("clickId")
                            ]
                        )
                if shareholderList:
                    for shareholder_list in shareholderList:
                        myWriter6.writerow(
                            [
                                companyid,
                                company_name,
                                do_time,
                                shareholder_list.get("investorName"),
                                shareholder_list.get("subscribeAmount"),
                                shareholder_list.get("subscribeTime"),
                                shareholder_list.get("subscribeType"),
                                shareholder_list.get("paidAmount"),
                                shareholder_list.get("paidTime"),
                                shareholder_list.get("paidType"),
                                shareholder_list.get("type"),
                                shareholder_list.get("clickId"),
                                shareholder_list.get("reportYear")

                            ]
                        )
                if webInfoList:
                    for web_info_list in webInfoList:
                        myWriter7.writerow(
                            [
                                companyid,
                                company_name,
                                do_time,
                                web_info_list.get("reportYear"),
                                web_info_list.get("webType"),
                                web_info_list.get("name"),
                                web_info_list.get("website")
                            ]
                        )
                if reportSocialSecurityInfo:
                    myWriter8.writerow(
                        [
                            companyid,
                            company_name,
                            do_time,
                            reportSocialSecurityInfo.get("id"),
                            reportSocialSecurityInfo.get("annaulreportId"),
                            reportSocialSecurityInfo.get("endowmentInsurance") if reportSocialSecurityInfo.get("endowmentInsurance") else 0,
                            reportSocialSecurityInfo.get("unemploymentInsurance") if reportSocialSecurityInfo.get("unemploymentInsurance") else 0,
                            reportSocialSecurityInfo.get("medicalInsurance") if reportSocialSecurityInfo.get("medicalInsurance") else 0,
                            reportSocialSecurityInfo.get("employmentInjuryInsurance") if reportSocialSecurityInfo.get("employmentInjuryInsurance") else 0,
                            reportSocialSecurityInfo.get("maternityInsurance") if reportSocialSecurityInfo.get("maternityInsurance") else 0,
                            reportSocialSecurityInfo.get("endowmentInsuranceBase"),
                            reportSocialSecurityInfo.get("unemploymentInsuranceBase"),
                            reportSocialSecurityInfo.get("medicalInsuranceBase"),
                            reportSocialSecurityInfo.get("maternityInsuranceBase"),
                            reportSocialSecurityInfo.get("endowmentInsurancePayAmount"),
                            reportSocialSecurityInfo.get("unemploymentInsurancePayAmount"),
                            reportSocialSecurityInfo.get("medicalInsurancePayAmount"),
                            reportSocialSecurityInfo.get("employmentInjuryInsurancePayAmount"),
                            reportSocialSecurityInfo.get("maternityInsurancePayAmount"),
                            reportSocialSecurityInfo.get("endowmentInsuranceOweAmount"),
                            reportSocialSecurityInfo.get("unemploymentInsuranceOweAmount"),
                            reportSocialSecurityInfo.get("medicalInsuranceOweAmount"),
                            reportSocialSecurityInfo.get("employmentInjuryInsuranceOweAmount"),
                            reportSocialSecurityInfo.get("maternityInsuranceOweAmount"),
                            reportSocialSecurityInfo.get("createTime")
                        ]
                    )
    except:
        traceback.print_exc()
        print(com_name)

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    data_list = data_list[5750:6000]
    # print('获取公司条数', data_list, len(data_list))
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_baseInfo_neeq1.csv', 'w', encoding='utf-8', newline='') as myFile1, \
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_change_record_list_neeq1.csv', 'w', encoding='utf-8',
             newline='') as myFile2,\
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_equity_change_info_list_neeq1.csv', 'w', encoding='utf-8',
             newline='') as myFile3, \
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_out_guarantee_info_list_neeq1.csv', 'w', encoding='utf-8',
             newline='') as myFile4, \
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_out_bound_investment_list_neeq1.csv', 'w', encoding='utf-8',
             newline='') as myFile5, \
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_shareholder_list_neeq1.csv', 'w', encoding='utf-8',
             newline='') as myFile6, \
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_web_info_list_neeq1.csv', 'w', encoding='utf-8',
             newline='') as myFile7, \
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/annual_report_social_security_info_neeq1.csv', 'w', encoding='utf-8',
             newline='') as myFile8:
        myWriter1 = csv.writer(myFile1)
        myWriter2 = csv.writer(myFile2)
        myWriter3 = csv.writer(myFile3)
        myWriter4 = csv.writer(myFile4)
        myWriter5 = csv.writer(myFile5)
        myWriter6 = csv.writer(myFile6)
        myWriter7 = csv.writer(myFile7)
        myWriter8 = csv.writer(myFile8)
        count = 0
        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            # company_name = '北京久其软件股份有限公司'
            count += 1
            print('company_name', company_name, count)
            companyid = data_list[com_name][2]
            # print('获取公司名字',data_list[com_name][1])
            page = requestsPage(page_i, company_name)
            if page != False:
                requestsData(page,
                             company_name,
                             companyid,
                             myWriter1,
                             myWriter2,
                             myWriter3,
                             myWriter4,
                             myWriter5,
                             myWriter6,
                             myWriter7,
                             myWriter8)

