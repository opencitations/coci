
# coding: utf-8

# In[20]:

import os
from subprocess import Popen
from argparse import ArgumentParser



if __name__ == "__main__":
    arg_parser = ArgumentParser("multi_cociprocess.py", description="Assign a process to handle each collection of crossref JSON files saved in a subdirectory.")

    arg_parser.add_argument("-pycmd", dest="pycmd", required=False, help="The python 3 bin path.")
    arg_parser.add_argument("-script", dest="script_full_path", required=True, help="The script full path(with its name).")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the crossref data dump.")
    arg_parser.add_argument("-out", "--output_dir", dest="output_dir", required=True, help="The root directory where results will be stored.")
    arg_parser.add_argument("-lookup", dest="lookup_file_path", required=False, help="The lookup file full path (with file name).")
    args = arg_parser.parse_args()

    CMD_PY = "/anaconda/envs/py35/bin/python3.5"
    SCRIPT_FULL_PATH = "/srv/dev/coci/script/cociprocess.py"
    INPUT_ROOT_DIR = '/usr/share/'
    OUTPUT_ROOT_DIR = ''
    LOOKUP_FILE = 'lookup.csv'
    #"/srv/data/coci/open/"

    if args.pycmd:
        CMD_PY = args.pycmd

    if args.script_full_path:
        SCRIPT_FULL_PATH = args.script_full_path

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    if args.output_dir:
        OUTPUT_ROOT_DIR = args.output_dir

    if args.lookup_file_path:
        LOOKUP_FILE = args.lookup_file_path

    list_subprocesses = []
    for dirname, dirnames, filenames in os.walk(INPUT_ROOT_DIR):
        for subdirname in dirnames:
            input_full_path = os.path.join(dirname, subdirname)
            output_full_path = '%s%s'%(OUTPUT_ROOT_DIR, subdirname)

            subprocess_val = '%s %s -in %s -out %s -lookup %s'%(CMD_PY, SCRIPT_FULL_PATH, input_full_path, output_full_path,LOOKUP_FILE)
            list_subprocesses.append(subprocess_val)
            #os.system('%s %s -in %s -out %s -lookup %s'%(CMD_PY, SCRIPT_FULL_PATH, input_full_path, output_full_path,LOOKUP_FILE))

    processes = [Popen(cmd, shell=True) for cmd in list_subprocesses]
    for p in processes: p.wait()
    










# In[ ]: