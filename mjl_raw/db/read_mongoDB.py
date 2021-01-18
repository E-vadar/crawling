# -*- coding:utf-8 -*-

"""
@Time ： 2020/12/3 11:41 AM
@Auth ： Stone
@File ：read_mongoDB.py.py
@IDE  ：PyCharm
"""

# 读取MongoDB数据
from pymongo import MongoClient
import configparser
import os

current_file_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
config_path = os.path.join(current_file_path, "conf/config.ini")

config = configparser.ConfigParser()
config.read(config_path)

class MongoDBHelper(object):
    def __init__(self):
        self.mongo_host = config.get('MONGODB', 'host')
        self.mongo_username = config.get('MONGODB', 'username')
        self.mongo_password = config.get('MONGODB', 'password')
        self.mongo_dbname = config.get('MONGODB', 'db_name')
        self.mongo_port = config.get('MONGODB', 'port')

        self.client = MongoClient(host=self.mongo_host, port=int(self.mongo_port))
        self.db = self.client[self.mongo_dbname]
        self.db.authenticate(self.mongo_username, self.mongo_password, mechanism='SCRAM-SHA-1')

    def get_state(self):
        return self.client is not None and self.db is not None

    def insert_one(self, collection, data):
        if self.get_state():
            ret = self.db[collection].insert_one(data)
            return ret.inserted_id
        else:
            return ""

    def insert_many(self, collection, data):
        if self.get_state():
            # 插入多条数据，data需为list
            ret = self.db[collection].insertMany(data)
            return ret.inserted_id
        else:
            return ""

    def update(self, collection, data):
        # data format:
        # {key:[old_data,new_data]}
        data_filter = {}
        data_revised = {}
        for key in data.keys():
            data_filter[key] = data[key][0]
            data_revised[key] = data[key][1]
        if self.get_state():
            return self.db[collection].update_many(data_filter, {"$set": data_revised}).modified_count
        return 0

    def find(self, col, condition, column=None):
        if self.get_state():
            if column is None:
                rt = self.db[col].find(condition)
                result = []
                for i in rt:
                    result.append(i)
                return result
                # return self.db[col].find(condition)
            else:
                return self.db[col].find(condition, column)
        else:
            return None

    def find_tables(self):
        if self.get_state():
            return self.db.collection_names()
        else:
            return None

    def find_data_limit_skip(self, col, limit_num, skip_num=0):
        if self.get_state():
            return self.db[col].find().limit(limit_num).skip(skip_num)
        else:
            return None

    def delete(self, col, condition):
        if self.get_state():
            return self.db[col].delete_many(filter=condition).deleted_count
        return 0

if __name__ == '__main__':
    db = MongoDBHelper()
    # db = MongoDBHelper("192.168.88.106", 27027, "test", "beijingipo", "beijingipo123!")
    data = {"companyName":"上海拉夏贝尔服饰股份有限公司"}
    
    col_list = db.find("TianyanchaZhixingInfo", data, {"result":1,"_id":0})
    # print(col_list)
    for i in col_list:
        print(i)
        print("-----------")
    # print(db.get_state())
    # collection_list = db.find_tables()
    # print("表的个数是",len(collection_list))
    # for collection in collection_list:
    #     print(collection)
    #     print("表名是:{}----条数是：{}".format(collection, db.find(collection,{}).count()))
