# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import re
import time
from datetime import datetime
from pyspark import SparkContext, SparkConf



def check_head_of_message(line):
    result = re.search(r'{2:[O|I]\S{3}',line)
    if result is not None:
        return 0
    else:
        return -1

# def split(pair):
#     head, content = pair
#     raw_messages = [line for line in re.split(r'{1:',content) if line is not None]
#     messages = [ '{1:' + line.rstrip() + '<PIPE>' for line in raw_messages ]
#     return messages

def find(message):
    result = re.search(r'{2:[O|I]\S{3}',message)
    if result is not None:
        message_direction = result.group(0)[2]
        key = result.group(0)[3:]
        if key in key_set:
            return True
        else:
            return False
    else:
        return False


def filter1(message):
    matched = fine(message)
    if matched:
        return '{1:' + message.rstrip() + '|'
    else:
        return ''



def filter2(message):
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

    

# conf = SparkConf().setAppName('test_pyspark').setMaster("yarn-client")
# sc = SparkContext(conf=conf)
sc = SparkContext()

# filename='sample400M.txt'
# filename='sample400M.txt'
# filename='SAMPLE.txt'

filename = 'sample4G.txt'
source_path = '' + filename

dest_path = '/tmp/filter1_'+filename

key_set = ['540','541','542','543','545']


rdd1 = sc.newAPIHadoopFile(
    source_path,
    'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
    'org.apache.hadoop.io.Text',
    'org.apache.hadoop.io.LongWritable',  
    conf={'textinputformat.record.delimiter': '{1:'}
)

# rdd2 = rdd1.map(lambda m: '{1:'+m[1].rstrip() +'|' if m[1] is not None else None)
# rdd2 = rdd1.map(lambda m: filter_message(m[1]))


rdd2 = rdd1.map(lambda m: filter2(m[1]) ).filter(lambda m: m)

rdd2.coalesce(1).saveAsTextFile(dest_path)

# rdd1.saveAsTextFile(dest_path)
# rdd2 = rdd1.filter(lambda message: find(message))

# rdd2.saveAsTextFile(dest_path)


# textFile = sc.textFile("SAMPLE.txt")
# textFile = sc.wholeTextFiles(source_path)

# lines = textFile.collect()
# print('filtering start at {0}'.format(datetime.now()))
# start=time.time()
# textFile.flatMap(lambda x:split(x)).filter(lambda message:find(message)).saveAsTextFile(dest_path)
# end = time.time()
# print('Total time cost is {0}'.format(end-start))
# print('Task end at {0}'.format(datetime.now()))

# result_rdd = sc.parallelize(result).saveAsTextFile('filter_sample.txt')
