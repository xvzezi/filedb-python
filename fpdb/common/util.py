# coding=utf-8

###########################
# file: util.py
# date: 2021-7-16
# author: Sturmfy
# desc: utility
# version:
#   2021-7-16 init design
###########################

def path_stitch(dir_name, file_name):
    # type: (str, str) -> str
    if not dir_name.endswith('/'):
        dir_name = dir_name + '/'
    return dir_name + file_name