
import os
from argparse import ArgumentParser
from citation import Citation
from rdflib.namespace import RDF, RDFS, SKOS
import csv


if __name__ == "__main__":
    arg_parser = ArgumentParser("coci_rdfgen.py", description="Create the RDF dataset for COCI")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the COCI processed data")
    arg_parser.add_argument("-out", dest="output_file", required=False, help="The file where the dataset will be stored.")
    args = arg_parser.parse_args()

    INPUT_ROOT_DIR = "."
    OUTPUT_FILE = "dataset.nt"

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    if args.output_file:
        OUTPUT_FILE = args.output_file
        if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
            try:
                os.makedirs(os.path.dirname(OUTPUT_FILE))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

    with open(OUTPUT_FILE, 'w') as f:
        pass


    all_dirs = [ os.path.join(INPUT_ROOT_DIR, name) for name in os.listdir(INPUT_ROOT_DIR) if os.path.isdir(os.path.join(INPUT_ROOT_DIR, name)) ]
    base_url = "https://w3id.org/oc/index/coci/"
    agent_url = "https://w3id.org/oc/index/coci/prov/pa/1"
    doi_prefix = "http://dx.doi.org/"


    print("The Root directory is %s"%(INPUT_ROOT_DIR))
    for d in all_dirs:
        print("Processin %s"%(d))
        data_dir = '%s/data/'%(d)
        prov_dir = '%s/prov/'%(d)

        #populate prov dictionary
        prov_dic = {}
        for dirpath, dnames, fnames in os.walk(prov_dir):
            for f in fnames:
                if f.lower().endswith('.csv'):
                    p_file_path = os.path.join(prov_dir, f)
                    #open csv and update prov dictionary
                    with open(p_file_path,'r') as csvfile:
                        csv_reader = csv.DictReader(csvfile)
                        for row in csv_reader:
                            prov_dic[row['oci']] = {'agent': row['agent'],'source': row['source'],'datetime': row['datetime']}

        #iterate all data entries
        for dirpath, dnames, fnames in os.walk(data_dir):
            for f in fnames:
                if f.lower().endswith('.csv'):
                    d_file_path = os.path.join(data_dir, f)

                    #open the csv file and process each data entry
                    block_txt = ""
                    with open(d_file_path,'r') as csvfile:
                        csv_reader = csv.DictReader(csvfile)
                        for row in csv_reader:
                            # oci , citing , cited , creation , timespan
                            # get the prov data from the prov dictionary oci key
                            # create the rdf and generate the files
                            oci_prov = {'agent': None,'source': None,'datetime': None}
                            if row['oci'] in prov_dic:
                                oci_prov = prov_dic[row['oci']]

                            citation = Citation( None, doi_prefix + row['citing'], None,
                                                None, doi_prefix + row['cited'], None,
                                                agent_url, oci_prov['source'], oci_prov['datetime'],
                                                creation_date= row['creation'], duration= row['timespan'], oci= row['oci'])

                            g = citation.get_citation_rdf(base_url, include_oci=False, include_id_link=False, include_rdfs_lbl=False)
                            block_txt = block_txt + str(g.serialize(format='nt'))

                    if block_txt != "":
                        with open(OUTPUT_FILE, 'a', newline='') as f:
                            f.write(block_txt)
    print("Done")
