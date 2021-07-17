# coding=utf-8
from fpdb import DB
from fpdb import table

def test():
    db = DB('data')
    k = db.create('test2')
    print k.insert({'vale':1})
    print k.insert({'id':1, 'name':'asd', 'v':0})
    print k.find({'v':table.Filter.LESS(1)})
    k.close()
    db.shutdown()

    db = DB('data')
    k = db.find('test2')
    print k.update({'id':1, 'name':'asd', 'v':1})
    print k.find({'v':table.Filter.LESS(1)})
    k.close()
    db.shutdown()

def main():
    test()

if __name__ == "__main__":
    main()