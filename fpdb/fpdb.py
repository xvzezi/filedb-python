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
1. db = FPDB(path)
2. table = db.Find('table')
3. model = table.Create({'key':''})
4. model = table.Find('key')
5. model.attr = value
6. model.update()
"""