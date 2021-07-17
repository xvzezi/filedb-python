# coding=utf-8

###########################
# file: table.py
# date: 2021-7-16
# author: Sturmfy
# desc: wrapper of the FileJson
# version:
#   2021-7-16 init design
###########################

from .cache import JsonFile

class FilterType:
    EQUAL =    0
    LESS =     1
    LESSEQ =   2
    GRTR =     3
    GRTREQ =   4
    NOT =      5
    OR =       6
    AND =      7

class Filter(object):

    def __init__(self):
        super(Filter, self).__init__()
        self.__lval = None 
        self.__rval = None 
        self.__type = None 
    
    def eval(self, val):
        lv = self.__lval
        if isinstance(lv, Filter):
            lv = lv.eval(val)
        rv = self.__rval
        if isinstance(rv, Filter):
            rv = rv.eval(val)
        # print 'eval', self.__type, lv, rv, val
        if self.__type == FilterType.NOT:
            return not rv 
        elif self.__type == FilterType.OR:
            return lv or rv 
        elif self.__type == FilterType.AND:
            return lv and rv 
        elif self.__type == FilterType.EQUAL:
            return val == rv
        elif self.__type == FilterType.LESS:
            return val < rv
        elif self.__type == FilterType.LESSEQ:
            return val <= rv
        elif self.__type == FilterType.GRTR:
            return val > rv
        elif self.__type == FilterType.GRTREQ:
            return val >= rv
        return False
    
    @classmethod
    def NOT(cls, tar):
        f = cls()
        f.__type = FilterType.NOT
        f.__rval = tar 
        return f

    @classmethod
    def OR(cls, tar1, tar2):
        f = cls()
        f.__type = FilterType.OR
        f.__lval = tar1
        f.__rval = tar2
        return f    

    @classmethod
    def AND(cls, tar1, tar2):
        f = cls()
        f.__type = FilterType.AND
        f.__lval = tar1
        f.__rval = tar2
        return f

    @classmethod
    def EQUAL(cls, tar):
        f = cls()
        f.__type = FilterType.EQUAL
        f.__rval = tar 
        return f 

    @classmethod
    def LESS(cls, tar):
        f = cls()
        f.__type = FilterType.LESS
        f.__rval = tar 
        return f 
    
    @classmethod
    def LESSEQ(cls, tar):
        f = cls()
        f.__type = FilterType.LESSEQ
        f.__rval = tar 
        return f 
    
    @classmethod
    def GRTR(cls, tar):
        f = cls()
        f.__type = FilterType.GRTR
        f.__rval = tar 
        return f 
    
    @classmethod
    def GRTREQ(cls, tar):
        f = cls()
        f.__type = FilterType.GRTREQ
        f.__rval = tar 
        return f 


class Table(object):
    def __init__(self, file):
        # type: (JsonFile) -> None
        super(Table, self).__init__()
        self.file = file
        if self.file.data.get('data', None) is None:
            self.file.data['data'] = []
        self.meta = None # type: dict
        self.unique = {}  # type: dict[str,bool]
        self.__meta_set()
    
    def __meta_set(self):
        self.file.lock()
        self.meta = self.file.data.get('__meta', None)
        if self.meta is not None:
            ul = self.meta.get('unique', [])
            for u in ul:
                self.unique[u] = True
        self.file.release()
        return 

    def need_init(self):
        return self.meta is None 

    def init_new_table(self, configs = {
        'unique':['id', 'name']}):
        if self.meta:
            return 
        self.file.lock()
        self.meta = configs
        self.file.data['__meta'] = self.meta 
        self.file.data['data'] = []
        self.file.release()
        self.__meta_set()
        self.file.flush()

    def find(self, conds):
        # type: (dict[str,Filter]) -> any
        res = []
        self.file.lock()
        for o in self.file.data['data']:
            is_ok = True
            for i in conds:
                kv = o.get(i, None)
                if kv is None:
                    is_ok = False
                    # print 'Key not found', i
                    break
                if not conds[i].eval(kv):
                    # print 'Values not satisfied', kv
                    is_ok = False
                    break 
            if is_ok:
                res.append(o)
        self.file.release()
        return res 

    def insert(self, data):
        # 1. check meta file
        for m in self.unique:
            if data.get(m, None) is None:
                print 'unique', m, 'not found'
                return False 
        
        # 2. find if have same meta
        query = {}
        for m in self.unique:
            query[m] = Filter.EQUAL(data[m])
        tar = self.find(query)
        if len(tar) > 0:
            print 'unexpected same unique', tar 
            return False

        # 2. insert
        self.file.lock()
        self.file.data['data'].append(data)
        self.file.release()
        self.file.flush()
        # print self.file.data
        return True

    def update(self, data):
        # 1. check meta file
        for m in self.unique:
            if data.get(m, None) is None:
                print 'unique', m, 'not found'
                return False 
        
        # 2. find the target
        query = {}
        for m in self.unique:
            query[m] = Filter.EQUAL(data[m])
        self.file.lock()
        for k in xrange(len(self.file.data['data'])):
            o = self.file.data['data'][k]
            is_ok = True
            for i in query:
                kv = o.get(i, None)
                if kv is None:
                    is_ok = False
                    break
                if not query[i].eval(kv):
                    is_ok = False
                    break 
            if is_ok:
                self.file.data['data'][k] = data 
                break 
        self.file.release()
        self.file.flush()
        return True

    def remove(self, conds):
        # type: (dict[str,Filter]) -> any
        res = []
        self.file.lock()
        for k in xrange(len(self.file.data['data'])):
            o = self.file.data['data'][k]
            is_ok = True
            for i in conds:
                kv = o.get(i, None)
                if kv is None:
                    is_ok = False
                    break
                if not conds[i].eval(kv):
                    is_ok = False
                    break 
            if is_ok:
                res.append(k)
        for i in xrange(len(res)-1, -1, -1):
            del self.file.data['data'][res[i]]
        self.file.release()
        self.file.flush()
        return res 
    
    def close(self):
        self.file.close()


if __name__ == "__main__":
    from cache import DBCache
    cache = DBCache('data')
    cache.create('test')
    f = cache.open('test')
    t = Table(f)
    t.init_new_table()
    print t.insert({
        'id':123,
        'name':'woaini',
        'value':'k'
    })
    print t.insert({
        'id':456,
        'name':'woaini2',
        'value':'k'
    })
    print t.insert({
        'id':178,
        'name':'woaini',
        'value':'k2'
    })
    print 'find: '
    print t.find({
        'id':Filter.OR(Filter.LESS(156), Filter.GRTR(200)),
    })
    print t.find({
        'id':Filter.OR(Filter.LESS(156), Filter.GRTR(200)),
        'value':Filter.NOT(Filter.EQUAL('k'))
    })
    print t.find({
        'name':Filter.EQUAL('woaini')
    })
    t.update({
        'id':123,
        'name':'woaini',
        'value':'k1',
        'hah':0
    })
    print 'remove'
    t.remove({
        'id':Filter.AND(Filter.GRTR(177), Filter.LESS(179))
    })
    t.close()
    cache.shutdown()

    