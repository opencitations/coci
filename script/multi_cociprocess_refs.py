

import os
from time import sleep
import subprocess
import multiprocessing
from subprocess import Popen
from argparse import ArgumentParser
from shutil import copyfile

def done(p):
    return p.poll() is not None

def success(p):
    return p.returncode == 0

def worker(cmd):
    subprocess.call(cmd, shell=True)

if __name__ == "__main__":
    arg_parser = ArgumentParser("multi_cociprocess.py", description="Assign a process to handle each collection of crossref JSON files saved in a subdirectory.")

    arg_parser.add_argument("-pycmd", dest="pycmd", required=False, help="The python 3 bin path.")
    arg_parser.add_argument("-script", dest="script_full_path", required=True, help="The script full path(with its name).")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the crossref data dump.")
    arg_parser.add_argument("-glob", dest="glob_dir", required=True, help="The root directory of the global index.")
    arg_parser.add_argument("-out", "--output_dir", dest="output_dir", required=True, help="The root directory where results will be stored.")
    arg_parser.add_argument("-p", dest="n_proc", required=False, help="The number of subprocess to generate.")
    #arg_parser.add_argument("-lookup", dest="lookup_file_path", required=False, help="The lookup file full path (with file name).")
    args = arg_parser.parse_args()

    CMD_PY = "python3.5"
    SCRIPT_FULL_PATH = "cociprocess_refs.py"
    INPUT_ROOT_DIR = ''
    OUTPUT_ROOT_DIR = ''
    GLOB_ROOT_DIR = ''
    #"/srv/data/coci/open/"

    CHECK_TIME = 300
    NUM_PROC = 32

    if args.n_proc:
        NUM_PROC = int(args.n_proc)

    if args.pycmd:
        CMD_PY = args.pycmd

    if args.script_full_path:
        SCRIPT_FULL_PATH = args.script_full_path

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    if args.glob_dir:
        GLOB_ROOT_DIR = args.glob_dir

    if args.output_dir:
        OUTPUT_ROOT_DIR = args.output_dir

    list_subprocesses = []
    for dirname, dirnames, filenames in os.walk(INPUT_ROOT_DIR):
        for subdirname in dirnames:
            input_full_path = os.path.join(dirname, subdirname)
            output_full_path = '%s%s'%(OUTPUT_ROOT_DIR, str(len(list_subprocesses) % NUM_PROC))

            subprocess_val = '%s %s -in %s -out %s -glob %s'%(CMD_PY, SCRIPT_FULL_PATH, input_full_path, output_full_path,GLOB_ROOT_DIR)
            list_subprocesses.append(subprocess_val)


    processes = [Popen(cmd, shell=True) for cmd in list_subprocesses]
    processes = []
    cmd_dic = {}
    proc_counter = 0
    last_cmd = 0
    for cmd in list_subprocesses:
        if proc_counter >= NUM_PROC:
            break
        new_p = Popen(cmd, shell=True, stdout=subprocess.PIPE, close_fds=True)
        #new_p = Popen(cmd, shell=True)
        std_out, std_err = new_p.communicate()
        processes.append(new_p)
        cmd_dic[str(new_p)] = cmd
        proc_counter = proc_counter + 1
        last_cmd = last_cmd + 1

    while True:
        print("Check processes ...")
        for p in processes:
            if done(p):
                if success(p):
                    processes.remove(p)
                    if last_cmd < len(list_subprocesses):
                        #create new subprocess and open it again
                        new_p = Popen(list_subprocesses[last_cmd],shell=True)
                        processes.append(new_p)
                        cmd_dic[str(new_p)] = list_subprocesses[last_cmd]
                        last_cmd = last_cmd + 1
                else:
                    print("Process %s done with err!"%(p))
                    processes.remove(p)
                    #create new subprocess and open it again
                    new_p = Popen(cmd_dic[str(p)],shell=True)
                    processes.append(new_p)
                    cmd_dic[str(new_p)] = cmd_dic[str(p)]



        sleep(CHECK_TIME)
