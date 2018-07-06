import os
import errno
from os.path import abspath, isdir, sep
from os import walk
from argparse import ArgumentParser

from SPARQLWrapper import SPARQLWrapper

def add(server, g_url, f_n):
    print("Add file", f_n)
    server = SPARQLWrapper(server)
    server.method = 'POST'
    server.setQuery('LOAD <file:' + abspath(f_n) + '> INTO GRAPH <' + g_url + '>')
    server.query()
    print("Done")

if __name__ == "__main__":
    arg_parser = ArgumentParser("coci_update_graph.py", description="Update COCI graph with a given input file of new triples.")
    arg_parser.add_argument("-s", "--server", dest="server_url", required=True, help="The server url.")
    arg_parser.add_argument("-in", "--input_file", dest="input_file", required=True, help="The absolute path to the dataset file.")
    arg_parser.add_argument("-g", "--graph", dest="graph_name", required=True, help="The graph name/url (where the input triples will be pushed).")

    args = arg_parser.parse_args()

    SERVER_URL = 'http://localhost:3001/blazegraph/sparql'
    INPUT_FILE = 'data_dataset.nt'
    GRAPH_URL = 'http://test.it/'

    if args.server_url:
        SERVER_URL = args.server_url

    INPUT_DIR = None
    INPUT_FILE = None

    if isdir(args.input_file):
        INPUT_DIR = args.input_file
    else:
        INPUT_FILE = args.input_file

    if args.graph_name:
        GRAPH_URL = args.graph_name

    if INPUT_DIR is None:
        add(SERVER_URL, GRAPH_URL, INPUT_FILE)
    else:
        for cur_dir, cur_subdir, cur_files in walk(INPUT_DIR):
            for cur_file in cur_files:
                if cur_file.endswith(".nt"):
                    add(SERVER_URL, GRAPH_URL, cur_dir + sep + cur_file)

