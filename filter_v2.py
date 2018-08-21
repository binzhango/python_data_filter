
import configparser
import re
import time
import argparse
from multiprocessing import Pool



parser = argparse.ArgumentParser(description='Process arguments')
parser.add_argument('-s', '--source', required=True, help="the file list all souce filename")

configParser = configparser.RawConfigParser() 
configFilePath = r'c.conf'
configParser.read(configFilePath)
source_path = configParser.get('default','source_path')
dest_path = configParser.get('default', 'dest_path')

type_key = ['101', '103', '195', '196', '198', '199', '200', '202', '203', '204', '205', '210', '295', '296', '298', '299', '304', '321', '350', '380', '395', '398', '399', '499', '527','540', '541', '542', '543','558', '564', '565', '578', '595', '596', '598', '599', '699', '700', '799', '899', '996', '998', '999']


def get_source_file_path(source_path, filename):
    source_file_path = source_path+'\\'+filename
    return source_file_path

def get_dest_file_path(dest_path, filename):
    dest_file_path = dest_path+'\\'+filename
    return dest_file_path


def check_end_of_message(line):
    return bool(re.search(r'^.*{5:{.*}}',line)) or bool(re.search(r'}}QU',line))

def check_type_of_message(line):
    result = re.search(r'2:[o|O]\S{3}',line)
    if result is not None:
        key = result.group(0)[3:]
        if key in type_key:
            return True
        else:
            return False
    else:
        return False

def filter(source, output):
    with open(source,'r') as in_file, open(output,'w') as out_file:
        messages = []
        match_finded = False
        for line in in_file:           
            messages.append(line)
            flag = check_type_of_message(line)
            if flag:
                match_finded = True
            if check_end_of_message(line):
                if match_finded:
                    # print(messages)
                    for message in messages:
                        out_file.write(message)
                match_finded = False
                messages.clear()

def get_keys(file_path):
    keys = {}
    with open(file_path, 'r') as fin:
        for line in fin:
            result = re.search(r'2:[o|O]\S{3}',line)
            if result is not None:
                key = result.group(0)[3:]
                if key in keys:
                    keys[key] +=1
                else:
                    keys[key] = 1
    return keys


def save_keys(filename,keys):
    new_filename = 'keys_'+filename
    with open(new_filename,'w') as fout:
        fout.write(str(keys))

def test(x):
    print(x)




if __name__ == '__main__':
    args = parser.parse_args()
    filenames = []
    with open(args.source, 'r') as fn:
        for line in fn:
            filenames.append(line.rstrip())
    
    # for filename in filenames:
    #     # source = get_source_file_path(source_path, filename)
    #     output = get_dest_file_path(dest_path, filename)
    #     print("start {}".format(filename))
    #     start = time.time()
    #     keys = get_keys(output)
    #     save_keys(filename,keys)
    #     end = time.time()
    #     print("end in {}".format(end -start))
        # print('%r'% source)
        # start = time.time()
        # print("filtering {}".format(filename))
        # # filter(source, output)
        # end = time.time()
        # print("Done! {} seconds".format(end-start),'\n')


    with Pool(processes=4) as pool:
        for filename in filenames:
            source = get_source_file_path(source_path, filename)
            pool.apply_async(test, (filename,)).get()






