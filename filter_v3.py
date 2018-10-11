# coding: utf-8

import re
import time
import argparse
from multiprocessing import Pool
from datetime import datetime
import sys
# import logging



# create logger

# logger = logging.getLogger('filtering')
# logger.setLevel(logging.DEBUG)



parser = argparse.ArgumentParser(description='Process arguments')
parser.add_argument('-s', '--source', required=True, help='filename list')
args = parser.parse_args()

source_path = r''
dest_path = r''


key_set = ['540','541','542','543']


def get_source_file_path(source_path, filename):
    source_file_path = source_path+'\\'+filename
    return source_file_path


def get_dest_file_path(dest_path, filename):
    dest_file_path = dest_path+'\\'+filename
    return dest_file_path


def check_head_of_message(line):
    result = re.search(r'2:[O|I]\S{3}',line)
    if result is not None:
        return 0
    else:
        return -1


def check_type_of_message(line):
    result = re.search(r'2:[O|I]\S{3}',line)
    if result is not None:
        message_direction = result.group(0)[2]
        key = result.group(0)[3:]
        if key in key_set:
            if message_direction == 'O':
                return 0
            else:
                return 1
        else:
            return -1
    else:
        return -1 # useless message


def check_receiving_bic(line):
    result = re.search(r'SBOSUS3U.IMS', line)
    if result is not None:
        return 0
    else:
        return -1


def check_sending_bic(line):
    result = re.search(r'BBHCUS3I.AQR',line)
    if result is not None:
        return 0
    else:
        return -1


def filter(source, output):
    with open(source,'r') as in_file, open(output,'w') as out_file:
        messages = []
        global matched
        global line
        matched = True #targe message
        
        for line in in_file:
            isHead = check_head_of_message(line)

            type_flag, receive_flag, sending_flag = -1, -1, -1

            if isHead == 0: #flush previous messages[]
                if len(messages) > 0:
                    messages[-1] = messages[-1].rstrip()+'|'+'\n'
                for message in messages:
                    out_file.write(message)
                messages.clear()
                
                type_flag = check_type_of_message(line)
                receive_flag = check_receiving_bic(line)
                sending_flag = check_sending_bic(line)
                matched = (type_flag ==0 or type_flag ==1) and (receive_flag == 0) and (sending_flag == 0)
            if matched:
                messages.append(line)      

        if messages:
            messages[-1]=messages[-1].rstrip()+'|'+'\n' 
            for message in messages:
                out_file.write(message)
            messages.clear()


def main(filename):
    try:
        source = get_source_file_path(source_path, filename)
        output = get_dest_file_path(dest_path, 'filter_'+filename)
        start = time.time()
        print("Start filtering {} at {}".format(filename, str(datetime.now())))
        filter(source, output)
        end = time.time()
        print("{} Done! {} seconds".format(filename,(end-start),'\n'))
    except Exception as e:
        print(e)

if __name__ == '__main__':
    filenames = []
    with open(args.source, 'r') as fn:
        for line in fn:
            filenames.append(line.strip())
    with Pool(processes=4) as pool:
        pool.map(main, filenames)








