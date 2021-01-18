# -*- coding:utf-8 -*-

"""
@Time ： 2020/12/16 5:20 PM
@Auth ： Stone
@File ：publicnoticeInfo.py
@IDE  ：PyCharm
"""

# PUBLICNOTICE_INFO(796,"publicnoticeInfo"),   公示催告

import os, sys
# project_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
# # print(rootPath)
# sys.path.append(project_path)
# sys.path.append("/home/bbders/mengjinlei/company_crawl")
sys.path.append("/Users/mengjinlei/Desktop/python-project/company/company_crawl")

import json, random, pyhdfs, time
from util.web_request import WebRequest
import configparser
import pandas as pd
from datetime import datetime
from db.DBHelper import MysqlHelper, HiveHelper

class PublicNoticeInfo(object):

    def __init__(self):
        current_file_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        config_path = os.path.join(current_file_path, "conf/config.ini")

        config = configparser.ConfigParser()
        config.read(config_path)

        self.request_url = config.get('RUNNING', 'base_url')
        self.post_data = {
                "trdDataApi": "PUBLICNOTICE_INFO",
                "trdDataProvider": "TIANYANCHA",
                "trdDataRequest": {
                    "name": "北京百度网讯科技有限公司",
                },
                # "companyId": "",
                # "companyName": "北京百度网讯科技有限公司",
                "version": ""
            }
        # self.mysql_sql_param = config.get('MYSQL', 'param_sql')

    # 获取上市公司信息
    def get_company_list(self):
        sql = "select company_name,qyxx_id from company_manager where `status` like '上市%' "
        try:
            com_list = MysqlHelper().get_all(sql)
            return com_list
        except Exception as err:
            print("MYSQL查询数据时出错了~错误内容为{}".format(str(err)))

    def get_response(self, company_name, qyxx_id=None, pageNum = 1):
        self.post_data['trdDataRequest']['name'] = company_name
        self.post_data['trdDataRequest']['pageNum'] = pageNum
        self.post_data['companyName'] = company_name
        self.post_data['companyId'] = qyxx_id
        resp_text = WebRequest().post_data_json(self.request_url, data=json.dumps(self.post_data)).text
        resp_json = json.loads(resp_text)
        print(resp_json)
        return resp_json

    def is_paging(self, response):
        msg_data_code = response.get('data').get('code')
        page_num = 0
        # print(msg_data_code)
        if msg_data_code != '0':
            return page_num
        else:
            total_count = response.get('data').get('page').get('total')
            # print(total_count)

            if 1 <= total_count <= 20:
                #  print("数据是大于等于1小于等于20的，只获取一次")
                page_num = 1
            else:
                # print("数据是大于20,循环爬取前5页内容")
                import math
                page_num = math.ceil(int(total_count)/20)
            return page_num

    def deal_json(self, dict_key, json_dict):
        # 遍历字典: key值是否存在，如果存在取数据，不存在返回空
        if dict_key in json_dict:
            return json_dict[dict_key]
        else:
            return ''

    def parse_data(self, response, company_name, qyxx_id):
        result_dict = response.get('data').get('result')
        result_frame = []
        if result_dict == '':
            print("====={}--此公司没有信息=====".format(company_name))
        else:
            # print("舆情有内容，开始格式化！")
            for result in json.loads(result_dict):
                result_frame.append([
                    response.get('data').get('noSqlId'),
                    qyxx_id,
                    company_name,
                    datetime.now().strftime("%Y-%m-%d"),

                    self.deal_json('infoDetail', result),
                    self.deal_json('billType', result),
                    self.deal_json('exigentType', result),
                    self.deal_json('billAmt', result),
                    self.deal_json('ownerCompanyId', result),
                    self.deal_json('drawCompanyName', result),
                    self.deal_json('billNum', result),
                    self.deal_json('ownerCompanyName', result),
                    self.deal_json('id', result),
                    self.deal_json('payCompanyName', result),
                    self.deal_json('billBeginDt', result),
                    self.deal_json('applyCompanyId', result),
                    self.deal_json('publishDt', result),
                    self.deal_json('applyCompanyName', result),
                    self.deal_json('drawCompanyId', result),
                    self.deal_json('publishOrgName', result),

                    datetime.now().strftime("%Y-%m-%d")
                ])
            return result_frame

    def save_to_csv(self, opinion_frame):
        try:
            if opinion_frame is None:
                return
            else:
                df = pd.DataFrame(opinion_frame)
                file_name = 'publicnoticeInfo.csv'
                df.to_csv(file_name, sep=',', columns=None, index=False, header=False, mode='a', encoding='utf-8-sig')
                print("保存公司信息成功！")
                # return file_name
        except Exception as e:
            print("保存--公司信息失败！失败原因{}".format(str(e)))

    def execute_hive_sql(self, file_name):
        sql = """
                LOAD DATA INPATH 'hdfs://192.168.88.101:8020/home/data/parse_data/{}' OVERWRITE  
                INTO TABLE new_trd_data.punishmentinfo PARTITION(createddate='2020-12-01')
              """.format(file_name)
        try:
            HiveHelper().execute_hive_SQL(sql)
            print("数据load成功！")
        except Exception as e:
            print("HIVE数据库执行语句时出错了~错误内容为{}".format(str(e)))

    # 获取HDFS连接
    def get_hdfs_conn_upload_hdfs(self, file_name):
        c_num = 0
        local_path = '/Users/mengjinlei/Desktop/python-project/company/parse_tyc/{}'.format(file_name)
        hdfs_path = '/home/data/parse_data/{}'.format(file_name)
        try:
            client = pyhdfs.HdfsClient(hosts="192.168.88.101:50070",user_name="root")
            print("HDFS-client 已连接成功!")
            client.copy_from_local(local_path, hdfs_path)
            c_num += 1
        except Exception as e:
            print("HDFS链接出错，链接的错误原因为{}".format(str(e)))
        return c_num

    def run_main(self):
        com_list = self.get_company_list()

        count = 0
        # 定义一个空的DataFrame(), 方便读取数据累加保存
        hive_data_frame = pd.DataFrame()
        for com in com_list:
            # 根据公司进行获取基本信息
            resp_json = self.get_response(com[0])
            count += 1
            print("==========正在爬取第{}个公司==========".format(count))
            # 获取是否有数据，进行分页
            page_number = self.is_paging(resp_json)
            if page_number != 0:
                for page in range(1, int(page_number) + 1):
                    print("爬取-{}-第{}页".format(com[0], page))
                    time.sleep(1)
                    hive_data = self.parse_data(self.get_response(com[0], com[1], page), com[0],
                                                           com[1])
                    # print(hive_data)
                    hive_data_frame = hive_data_frame.append(hive_data)

                    if hive_data_frame.shape[0] > 0:
                        self.save_to_csv(hive_data_frame)
                        hive_data_frame.drop(hive_data_frame.index, inplace=True)

            else:
                print("=={}-没有数据==".format(com[0]))

        # print("hive_data_frame的行数为-{}-".format(hive_data_frame.shape[0]))
        # if hive_data_frame.shape[0] > 0:
        #     # 实现文件保存CSV
        #     self.save_to_csv(hive_data_frame)
        #     file_name_str = self.save_to_csv(hive_data_frame)
        #     # 实现数据写入HDFS
        #     cl_num = self.get_hdfs_conn_upload_hdfs(file_name_str)
        #     if cl_num > 0:
        #         print("success")
        #     else:
        #         print("failure")
        #     # 执行HIVE-SQL，数据load进hive数据库lawsuitinfo表
        #     self.execute_hive_sql(file_name_str)
        #     # 清空DataFrame
        #     hive_data_frame.drop(hive_data_frame.index, inplace=True)

if __name__ == '__main__':
    public_notice_info = PublicNoticeInfo()
    public_notice_info.run_main()
