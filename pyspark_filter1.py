# -*- coding: utf-8 -*-
from __future__ import print_function
import sys
import re
import time
import argparse
from datetime import datetime
from pyspark import SparkContext, SparkConf
from operator import add





def filter_message(message):
    result = re.search(r'{2:[O|I]\S{3}',message)
    if result is not None:
        message_direction = result.group(0)[3]
        key = result.group(0)[4:]
        if key in key_set:
            return '{1:' + message.rstrip() + '|'
        else:
            return 
    else:
        return


def generate_IO_report(message):
    result = re.search(r'{2:[O|I]\S{3}',message)
    message_direction = result.group(0)[3]
    key = result.group(0)[4:]
    return ((key, message_direction),1)


def toCSVLine(data):
    return data[0][0]+','+data[0][1]+','+data[1]



# def filter_receiving_bic(message,receiving_bic_type):
#     result = re.search(r'{1:\S\d+\S{12}', line)
#     if result is not None:
#         receiving_bic = result.group(0)[-12:]
#         if receiving_bic == receiving_bic_type:
#             return True
#         else:
#             return False
#     else:
#         False


# def filter_sending_bic(message,sending_bic_type):
#     result = re.search(r'2:[O|I]\d+\S{12}',line)
#     if result is not None:
#         sending_bic = result.group(0)[-12:]
#         if sending_bic == sending_bic_type:
#             return True
#         else:
#             return False
#     else:
#         return False








parser = argparse.ArgumentParser(description='Process arguments')
parser.add_argument('-f', '--filename', required=True, help='filename')
# parser.add_argument('-r', '--receiving_bic',help='receiving_bic')
# parser.add_argument('-s', '--sending_bic',help='sending_bic')
args = parser.parse_args()

filename = args.filename


source_path = '/' + filename

dest_path = '/filter1_'+filename

key_set = ['101', '103', '195', '196', '198', '199', '200', '202', '203', '204', '205', '210', '295', '296', '298', '299', '304', '321', '350', '380', '395', '398', '399', '499', '527','540', '541', '542', '543','558', '564', '565', '578', '595', '596', '598', '599', '699', '700', '799', '899', '996', '998', '999']


sc =  SparkContext()

source = sc.newAPIHadoopFile(
    source_path,
    'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
    'org.apache.hadoop.io.Text',
    'org.apache.hadoop.io.LongWritable',  
    conf={'textinputformat.record.delimiter': '{1:'}
)


type_matched = srouce.map(lambda m: filter_message(m[1])).filter(lambda x : x)

type_report = type_matched.map(lambda m: generate_IO_report(m)).reduceByKey(add).map(lambda x: toCSVLine(x)).coalesce(1).saveAsTextFile(dest_path)

# receiving_bic_matched=type_matched.filter_receiving_bic(lambda x:filter_receiving_bic(x,args.receiving_bic_type))

# sending_bic_matched = receiving_bic_matched.filter_sending_bic(lambda x: filter_sending_bic(x, args.sending_bic_type))



