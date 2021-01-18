import traceback
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

def requestsPage(i, company_name):
    data = {
        "trdDataApi": "PROJECT_INFO",
        "trdDataProvider": "TIANYANCHA",
        "trdDataRequest": {
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
    state=json.loads(response.text)
    # print('state_one', state)
    if state.get('data').get('successFlag') == True:
        return [i.get("businessId") for i in json.loads(state.get('data').get('result'))]
    else:
        print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        return False

def requestsData(business_ids, data_list_dic, company_name, companyid, myWriter1,myWriter2,myWriter3,myWriter4,myWriter5):
    for i in business_ids:
        data = {
            "trdDataApi": "PROJECT_DETAIL",
            "trdDataProvider": "TIANYANCHA",
            "trdDataRequest":{
                "businessId": i
            },
            "companyId": "",
            "companyName": company_name,
            "version": ""}
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)
        # page,state = requestsPage(i,company_name)
        # print('state', state)
        parse(state, data_list_dic, company_name, companyid, myWriter1, myWriter2, myWriter3, myWriter4, myWriter5)
        # return state

def parse(state,data_list_dic,company_name, companyid, myWriter1, myWriter2, myWriter3, myWriter4, myWriter5):
    try:
        result = json.loads(state.get('data').get('result'))
        # print(result)
        if (len(result)) != 0:
            proType = result.get("proType")
            proUse = result.get("proUse")
            proNum = result.get("proNum")
            projectLevel = result.get("projectLevel")
            provinceProNum = result.get("provinceProNum")
            buildNature = result.get("buildNature")
            creditCode = result.get("creditCode")
            buildCompany = result.get("buildCompany")
            projectDocumentNumber = result.get("projectDocumentNumber")
            proName = result.get("proName")
            totalInvestment = result.get("totalInvestment")
            base = result.get("base")
            totalArea = result.get("totalArea")
            bidding = result.get("bidding",{}).get("result")
            drawingReview = result.get("drawingReview",{}).get("result")
            contractFiling = result.get("contractFiling",{}).get("result")
            constructionPermit = result.get("constructionPermit",{}).get("result")
            completionAcceptanceRecord = result.get("completionAcceptanceRecord",{}).get("result")
            if bidding:
                for i in bidding:
                    myWriter1.writerow(
                        [companyid,
                         company_name,
                         do_time,
                        i.get('index'),
                        i.get('tenderType'),
                        i.get('tenderMethod'),
                        i.get('winningUnitName'),
                        i.get('winningDate'),
                        i.get('winningBidAmount'),
                        i.get('winningNoticeNumber'),
                        i.get('provincialWinningBidNumber'),
                        proType,
                        proUse,
                        proNum,
                        projectLevel,
                        provinceProNum,
                        buildNature,
                        creditCode,
                        buildCompany,
                        projectDocumentNumber,
                        proName ,
                        totalInvestment,
                        base,
                        totalArea
                 ])
            if drawingReview:
                for i in drawingReview:
                    myWriter2.writerow(
                        [companyid,
                         company_name,
                         do_time,
                        i.get('designUnitName'),
                        i.get('reviewCompletionDate'),
                        i.get('drawingCertificateNumber'),
                        i.get('surveyUnitName'),
                        i.get('index'),
                        i.get('drawingCertificateUnitName'),
                        i.get('provincialDrawingCertificateNumber'),
                        proType,
                        proUse,
                        proNum,
                        projectLevel,
                        provinceProNum,
                        buildNature,
                        creditCode,
                        buildCompany,
                        projectDocumentNumber,
                        proName ,
                        totalInvestment,
                        base,
                        totalArea
                 ])
            if contractFiling:
                for i in contractFiling:
                    myWriter3.writerow(
                        [companyid,
                         company_name,
                         do_time,
                        i.get('contractCategory'),
                        i.get('contractSigningDate'),
                        i.get('index'),
                        i.get('contractAmount'),
                        i.get('contractCecordNo'),
                        i.get('provincialContractCecordNo'),
                        proType,
                        proUse,
                        proNum,
                        projectLevel,
                        provinceProNum,
                        buildNature,
                        creditCode,
                        buildCompany,
                        projectDocumentNumber,
                        proName ,
                        totalInvestment,
                        base,
                        totalArea
                 ])
            if constructionPermit:
                for i in constructionPermit:
                    myWriter5.writerow(
                        [companyid,
                         company_name,
                         do_time,
                        i.get('area'),
                        i.get('index'),
                        i.get('constructionPermitNo'),
                        i.get('contractAmount'),
                        i.get('issueDate'),
                        i.get('provincialConstructionPermitNo'),
                        proType,
                        proUse,
                        proNum,
                        projectLevel,
                        provinceProNum,
                        buildNature,
                        creditCode,
                        buildCompany,
                        projectDocumentNumber,
                        proName ,
                        totalInvestment,
                        base,
                        totalArea
                 ])
            if completionAcceptanceRecord:
                for i in completionAcceptanceRecord:
                    myWriter4.writerow(
                        [companyid,
                         company_name,
                         do_time,
                        i.get('index'),
                        i.get('completionRecordNo'),
                        i.get('provincialCompletionRecordNo'),
                        i.get('actualCost'),
                        i.get('actualArea'),
                        i.get('actualStartDate'),
                        i.get('actualCompletionAcceptanceDate'),
                        proType,
                        proUse,
                        proNum,
                        projectLevel,
                        provinceProNum,
                        buildNature,
                        creditCode,
                        buildCompany,
                        projectDocumentNumber,
                        proName ,
                        totalInvestment,
                        base,
                        totalArea
                 ])
    except:
        traceback.print_exc()
        print(com_name)

if __name__ == '__main__':
    dx = get_con.sqlHelper()
    data_list = dx.get_one_data()
    data_list_dic = []
    with open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/qualification_detail_bidding.csv', 'w', encoding='utf-8', newline='') as myFile1, \
            open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/qualification_detail_drawing_review.csv', 'w',
                 encoding='utf-8', newline='') as myFile2,\
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/qualification_detail_contract_filing.csv', 'w', encoding='utf-8',
             newline='') as myFile3, \
            open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/qualification_detail_completion_acceptance_record.csv', 'w',
                 encoding='utf-8',  newline='') as myFile4,\
        open('D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/qualification_detail_construction_permit.csv', 'w',
             encoding='utf-8', newline='') as myFile5:
        myWriter1 = csv.writer(myFile1)
        myWriter2= csv.writer(myFile2)
        myWriter3 = csv.writer(myFile3)
        myWriter4 = csv.writer(myFile4)
        myWriter5 = csv.writer(myFile5)

        for com_name in range(len(data_list)):
            page_i = 1
            company_name = data_list[com_name][1]
            # company_name = '全维度测试有限责任公司'
            print('company_name', company_name)
            companyid = data_list[com_name][2]
            business_ids = requestsPage(page_i, company_name)
            if business_ids:
                requestsData(business_ids,
                             data_list_dic,
                             company_name,
                             companyid,
                             myWriter1,
                             myWriter2,
                             myWriter3,
                             myWriter4,
                             myWriter5,)
            # break
