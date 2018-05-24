import os
import errno
from os.path import abspath
from argparse import ArgumentParser

from SPARQLWrapper import SPARQLWrapper

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

    if args.input_file:
        INPUT_FILE = args.input_file

    if args.graph_name:
        GRAPH_URL = args.graph_name

    ## The sparql server
    server = SPARQLWrapper(SERVER_URL)
    server.method = 'POST'
    server.setQuery('LOAD <file:'+abspath(INPUT_FILE)+'> INTO GRAPH <'+GRAPH_URL+'>')
    server.query()

    print("Done")
