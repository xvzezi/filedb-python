# coding=utf-8

###########################
# file: cache.py
# date: 2021-7-16
# author: Sturmfy
# desc: file cache api
# version:
#   2021-7-16 init design
###########################

import threading 
import json 
import time
import util

class JsonFile(object):
    def __init__(self, cache, filename, data):
        super(JsonFile, self).__init__()
        self.data = data    # type: dict
        self.cache = cache # type: DBCache
        self.filename = filename # type: str
    
    def lock(self):
        self.cache.lock_file(self.filename)
    
    def release(self):
        self.cache.unlock_file(self.filename)

    def flush(self):
        self.lock()
        self.cache.flush_file(self.filename, self.data)
        self.release()        

    def close(self):
        self.flush()

class DBCache(object):

    def __init__(self, dir_name, interval=0.5):
        super(DBCache, self).__init__()
        self.dir_path = dir_name
        self.__files = {} # type: dict[str, object]
        self.__locks = {} # type: dict[str, threading.Lock]
        self.__last_updates = []
        self.__last_updates_lock = threading.Lock()
        self.interval = interval 
        self.timer = threading.Timer(self.interval, self.__cycle_flush)
        self.timer.start()

    def __load_file(self, file_name):
        path = util.path_stitch(self.dir_path, file_name)
        try:
            with open(path, 'r') as f:
                tar = json.load(f)
                self.__files[file_name] = tar 
                self.__locks[file_name] = threading.Lock()
            return True
        except IOError:
            return False

    def __flush_file(self, file_name):
        d = self.__files.get(file_name, None)
        if d is None:
            return 
        path = util.path_stitch(self.dir_path, file_name)
        with open(path, 'w') as f:
            json.dump(d, f, indent=4)
    
    def __cycle_flush(self):
        # print 'flushing'
        self.__last_updates_lock.acquire()
        vlu = self.__last_updates
        self.__last_updates = []
        self.__last_updates_lock.release()
        for u in vlu:
            self.__flush_file(u)
        self.timer = threading.Timer(self.interval, self.__cycle_flush)
        self.timer.start()

    def open(self, file_name):
        d = self.__files.get(file_name, None)
        if d is None:
            if not self.__load_file(file_name):
                return None 
            d = self.__files[file_name]
        return JsonFile(self, file_name, d)
    
    def create(self, file_name):
        d = {}
        self.__files[file_name] = d 
        self.__locks[file_name] = threading.Lock()
        self.flush_file(file_name, d)
    
    def flush_file(self, filename, data):
        self.__files[filename] = data 
    
        self.__last_updates_lock.acquire()
        self.__last_updates.append(filename)
        self.__last_updates_lock.release()
        return

    def lock_file(self, filename):
        l = self.__locks.get(filename, None)
        if l is not None:
            l.acquire()
    
    def unlock_file(self, filename):
        l = self.__locks.get(filename, None)
        if l is not None:
            l.release()

    def shutdown(self):
        time.sleep(self.interval * 2)
        self.timer.cancel()

if __name__ == "__main__":
    cache = DBCache('data')
    cache.create('test')
    f = cache.open('test')
    f.lock()
    f.data['test'] = 1
    f.release()
    f.close()
    time.sleep(3)
    cache.shutdown()