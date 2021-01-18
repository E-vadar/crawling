"""
文件说明：
历史股东信息

描述：
可以通过公司名称或ID获取企业历史的股东信息，历史股东信息包括股东名、出资比例、认缴金额、股东总数等字段信息

接口ID：877
"""

import requests
import json
import math
import sys
import pandas as pd
# from hdfs import Client
# import hdfs
# from hdfs.client import Client
from hdfs.client import Client
from hdfs import InsecureClient
import sys

from parse.conf import get_config

sys.path.append('C:/Users/cong/PycharmProjects/pythonProject/parse')
from lib import get_con
import time
# from hdfs.client import Client
# from lib import create_table_hive
# from lib import logHandler
import pyhdfs
# import uuid
import datetime
# from loguru import logger
# logger.add('C:\\Users\\cong\\PycharmProjects\\pythonProject\\parse\\log\\holderhistoryInfor.log',format="{time} {level} {message}", filter="",level="INFO")

def requestsPage(i,company_name):
    # company_name='珠海市优隆贸易有限公司'
    data = {"trdDataApi":"HOLDERHISTORY_INFO","trdDataProvider":"TIANYANCHA","trdDataRequest":
    {"id":"","name":company_name,"pageNum":i,'pageSize':'20'},
    "companyId":"","companyName":company_name,"version":""}
    # pageSize = 10
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url=' http://192.168.88.101:8901/trddata/getData', headers=headers, data=json.dumps(data))
    # #print(response.text)
    state=json.loads(response.text)
    #print('state_one',state)
    # if i==0:

    dataFalg = state.get('data').get('message')
    if state.get('data').get('successFlag') == True or dataFalg != '无数据':

        page = state.get('data').get('page').get('total')
        #print('get data num',page)
        return page
    else:
        # logger.error('%s 公司无法获取数据,requestsPage函数报错 %s'%(company_name,state.get('data').get('code')))
        # print('此公司无法获取数据,报错 %s'%(state.get('data').get('code')))
        print("%s 公司无法获取数据,requestsPage函数报错 %s'%(company_name,state.get('data').get('code'))")
        return False

def requestsData(page,data_list_dic,company_name,companyid,local_path):
    page=math.ceil(page/20)
    for i in range(page):
        # i = 1728
        i=i+1
        data = {"trdDataApi": "HOLDERHISTORY_INFO", "trdDataProvider": "TIANYANCHA", "trdDataRequest":
            {"id": "", "name": company_name,
             "pageNum": i},
                "companyId": "", "companyName": company_name, "version": ""}
        # pageSize = 10
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=' http://192.168.88.103:8901/trddata/getData', headers=headers,
                                 data=json.dumps(data))
        state = json.loads(response.text)

        dataFalg = state.get('data').get('message')
        if state.get('data').get('successFlag') == True or dataFalg != '无数据':

            parse(state,data_list_dic,companyid,company_name,local_path)
            # return state
        else:
            # logger.error('%s 公司无法获取数据,requestsData函数报错 %s' % (company_name, state.get('data').get('code')))
            #print('此公司无法获取数据,报错 %s' % (state.get('data').get('code')))
            print("'%s 公司无法获取数据,requestsData函数报错 %s' % (company_name, state.get('data').get('code'))")
            return False

def writr_to_csv(df,local_path):
    # filename = uuid.uuid4()
    # tmp_file_name = 'holderinfo_%s.csv' % filename
    # local_path="C:/Users/cong/PycharmProjects/pythonProject/parse/doc/csv_file/%s"%tmp_file_name
    df.to_csv(local_path,sep=',',columns=None, header=False, index=False,mode='a', encoding='utf-8-sig')
    return local_path,tmp_file_name
# 获取HDFS连接
def get_hdfs_conn():
    client = None
    try:
        # client = Client("http://192.168.88.101:50070", root='/')
        client = pyhdfs.HdfsClient(hosts='192.168.88.101:50070', user_name="root")
        #print('client',client)
    except Exception as e:
        print(e)
        # logger.error('链接hdfs报错 %s' % (e))

    return client
def append_write_tohdfs(client, df,local_path,tmp_file_name):
    '''
        第一次使用因为不存在文件，所以不能实现追加，只能实现覆盖
    '''
    #print('正在输入')
    #print('本地路径',local_path)
    time.sleep(2)
    hdfs_path = '/home/data/parse/%s'%tmp_file_name
    client.copy_from_local(local_path, hdfs_path)
def parse(state,data_list_dic,companyid,company_name,local_path):
    state_data = state
    data_dic = {}
    listData = []
    state = json.loads(state.get('data').get('result'))
    #print('leng',len(state))
    for i in range(len(state)):
        # print(state[i])
        capitals = state[i].get('capital')
        if capitals:
            for capital in capitals:
                # print(capital)
                data_dic['uid'] = state_data.get('data').get('noSqlId')
                data_dic['companyid'] = companyid
                data_dic['companyname'] = company_name
                data_dic['partition_rq'] = datetime.date.today()
                #print('解析日期', data_dic['partition_rq'])
                data_dic['amomon'] = capital.get('amomon')
                data_dic['time'] = capital.get('time')
                data_dic['percent'] = capital.get('percent')
                data_dic['paymet'] = capital.get('paymet')
                # data_dic['capital'] = state[i].get('capital')

                data_dic['toco'] = state[i].get("toco")
                data_dic['amount'] = state[i].get("amount")
                data_dic['id'] = state[i].get("id")
                data_dic['type'] = state[i].get("type")
                data_dic['name'] = state[i].get("name")

                listData.append(data_dic.copy())
                data_dic.clear()

    my_df = pd.DataFrame(listData)
    local_path, tmp_file_name = writr_to_csv(my_df,local_path)


if __name__ == '__main__':
    dx = get_con.sqlHelper()
    # data_list = dx.get_one_data_chaxun_xinsanban()
    data_list = dx.get_one_data()
    #print('获取公司条数',len(data_list))
    data_list_dic = []
    tmp_file_name = 'holderhistoryInfo.csv'
    # config = get_config.ConfigGet()
    # config.get_conf()
    # local_path = str(config.local_path)
    # local_path = "%s/%s" % (local_path, tmp_file_name)
    local_path = 'D:/qiyuanwork/pythonProject/parse/doc/csv_file_neeq/holder_history_info_pre.csv'

    for com_name in range(len(data_list)):
        #print('获取第几家公司',com_name)
        page_i = 1
        company_name = data_list[com_name][1]
        print('company_name',company_name)
        companyid = data_list[com_name][2]
        page = requestsPage(page_i,company_name)
        if page != False:
            requestsData(page,data_list_dic,company_name,companyid,local_path)
        else:
            print('获取下一家公司')

