# coding=utf-8

###########################
# file: fpdb.py
# date: 2021-7-16
# author: Sturmfy
# desc: file db python api
# version:
#   2021-7-16 init design
###########################

""" Usage
see: basic.py
see: common.table.py
see: common.cache.py
"""

from .common import cache
from .common import table 

class DB(object):
    def __init__(self, dir_path):
        super(DB, self).__init__()
        self.__cache = cache.DBCache(dir_path, interval=0.5)
    
    def create(self, table_name,  configs = {'unique':['id', 'name']}):
        jf = self.__cache.open(table_name)
        if jf is None:
            self.__cache.create(table_name)
            jf = self.__cache.open(table_name)
        t = table.Table(jf)
        if t.need_init():
            t.init_new_table(configs=configs)
        return t 

    def find(self, table_name):
        jf = self.__cache.open(table_name)
        if jf is None:
            return None 
        t = table.Table(jf)
        if t.need_init():
            return None 
        return t 
    
    def shutdown(self):
        self.__cache.shutdown()

