import re
import argparse
import time

parser = argparse.ArgumentParser(description='Process arguments')
parser.add_argument('-s', '--source', required=True, help="source filename")
parser.add_argument('-o', '--output', required=True, help="output filename")


type_key = ['101', '103', '195', '196', '198', '199', '200', '202', '203', '204', '205', '210', '295', '296', '298', '299', '304', '321', '350', '380', '395', '398', '399', '499', '527','540', '541', '542', '543','558', '564', '565', '578', '595', '596', '598', '599', '699', '700', '799', '899', '996', '998', '999']

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
 

if __name__ == '__main__':
    args = parser.parse_args()
    print("===filtering start===")
    start = time.time()
    filter(args.source, args.output)
    end = time.time()
    print("finished time is: {}".format(end-start))
