
import os
import errno
from argparse import ArgumentParser
from citation import Citation
from rdflib.namespace import RDF, RDFS, SKOS
import csv
import urllib
from io import StringIO


def populate_data(s):
    sf = StringIO(s)
    reader = csv.reader(sf, delimiter=',')

    block_txt = ''
    for row in reader:
        #s as a csv: oci,citing,cited,creation,timespan
        oci = row[0]
        citing = row[1]
        cited = row[2]
        creation = row[3]
        timespan = row[4]

        citation = Citation( None, doi_prefix +  urllib.parse.quote(citing), None,
                            None, doi_prefix +  urllib.parse.quote(cited), None,
                            None, None, None,
                            creation_date= creation, duration= timespan, oci= oci)

        g = citation.get_citation_rdf(BASE_URL, include_oci=False, include_id_link=False, include_rdfs_lbl=False, include_data=INCLUDE_DATA, include_prov=INCLUDE_PROV)
        block_txt = block_txt + g.serialize(format='nt').decode("utf-8")

    if block_txt != "":
        with open(OUTPUT_FILE, 'a', newline='') as f:
            f.write(block_txt)


def populate_prov(s):
    sf = StringIO(s)
    reader = csv.reader(sf, delimiter=',')

    block_txt = ''
    for row in reader:
        #s as a csv: oci,agent,source,datetime
        oci = row[0]
        agent = row[1]
        source = row[2]
        datetime = row[3]

        prov_query = urllib.parse.quote(source.replace(prov_source, ''))
        f_source = prov_source + prov_query

        citation = Citation( None, None, None,
                            None, None, None,
                            agent_url, f_source, datetime,
                            creation_date= None, duration= None, oci= oci)

        g = citation.get_citation_rdf(BASE_URL, include_oci=False, include_id_link=False, include_rdfs_lbl=False, include_data=INCLUDE_DATA, include_prov=INCLUDE_PROV)
        block_txt = block_txt + g.serialize(format='nt').decode("utf-8")

    if block_txt != "":
        with open(OUTPUT_FILE, 'a', newline='') as f:
            f.write(block_txt)


if __name__ == "__main__":
    arg_parser = ArgumentParser("coci_rdfgen.py", description="Create the RDF dataset for COCI")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the COCI processed data")
    arg_parser.add_argument("-out", dest="output_file", required=False, help="The absolute path to the new output file (.nt format).")
    arg_parser.add_argument("-data", action="store_true", dest="incdata", required=False, help="Specify wheter you want to include the data triples in the graph.")
    arg_parser.add_argument("-prov", action="store_true", dest="incprov", required=False, help="Specify wheter you want to include the provenance triples in the graph.")
    arg_parser.add_argument("-baseurl", dest="baseurl", required=False, help="The base url of the resources.")

    args = arg_parser.parse_args()

    INPUT_ROOT_DIR = "."
    OUTPUT_FILE = "dataset.nt"
    BASE_URL = "https://w3id.org/oc/index/coci/"
    INCLUDE_DATA = False
    INCLUDE_PROV = False

    if args.incdata:
        INCLUDE_DATA = True

    if args.incprov:
        INCLUDE_PROV = True

    if args.baseurl:
        BASE_URL = args.baseurl

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    if args.output_file:
        OUTPUT_FILE = args.output_file
        if os.path.dirname(OUTPUT_FILE) != "":
            if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
                try:
                    os.makedirs(os.path.dirname(OUTPUT_FILE))
                except OSError as exc:
                    if exc.errno != errno.EEXIST:
                        raise

    with open(OUTPUT_FILE, 'w') as f:
        pass


    all_dirs = [ os.path.join(INPUT_ROOT_DIR, name) for name in os.listdir(INPUT_ROOT_DIR) if os.path.isdir(os.path.join(INPUT_ROOT_DIR, name)) ]
    agent_url = "https://w3id.org/oc/index/coci/prov/pa/1"
    doi_prefix = "http://dx.doi.org/"
    prov_source = "https://api.crossref.org/works/"


    print("The Root directory is %s"%(INPUT_ROOT_DIR))

    #print("Processin %s"%(d))
    data_dir = '%s/data/'%(INPUT_ROOT_DIR)
    prov_dir = '%s/prov/'%(INPUT_ROOT_DIR)

    if INCLUDE_PROV:
        for dirpath, dnames, fnames in os.walk(prov_dir):
            for f in fnames:
                if f.lower().endswith('.csv'):
                    p_file_path = os.path.join(prov_dir, f)
                    with open(p_file_path) as fprov:
                        count = 0
                        s = ""
                        next(fprov)
                        for line in fprov:
                            if count < 10000:
                                count += 1
                                s += line
                            else:
                                populate_prov(s)
                                s = ""
                                count = 0

                        if s != "":
                            populate_prov(s)

    if INCLUDE_DATA:
        for dirpath, dnames, fnames in os.walk(data_dir):
            for f in fnames:
                if f.lower().endswith('.csv'):
                    p_file_path = os.path.join(data_dir, f)
                    with open(p_file_path) as fdata:
                        count = 0
                        s = ""
                        next(fdata)
                        for line in fdata:
                            if count < 10000:
                                count += 1
                                s += line
                            else:
                                populate_data(s)
                                s = ""
                                count = 0

                        if s != "":
                            populate_data(s)
