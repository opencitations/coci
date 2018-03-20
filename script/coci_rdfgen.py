
import os
from argparse import ArgumentParser
import citation

if __name__ == "__main__":
    arg_parser = ArgumentParser("coci_rdfgen.py", description="Create the RDF dataset for COCI")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the COCI processed data")

    INPUT_ROOT_DIR = "."

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    all_dirs = [ os.path.join(INPUT_ROOT_DIR, name) for name in os.listdir(INPUT_ROOT_DIR) if os.path.isdir(os.path.join(INPUT_ROOT_DIR, name)) ]


    print("The Root directory is %s"%(INPUT_ROOT_DIR))
    for d in all_dirs:
        print("Processin %s"%(d))
        data_dir = '%s/data'%(d)
        prov_dir = '%s/prov'%(d)

        #populate prov dictionary
        prov_dic = {}
        for files in os.walk(prov_dir):
            for file in files:
                if file.lower().endswith('.csv'):
                    p_file_path = os.path.join(prov_dir, file)
                    #open csv and update prov dictionary
                    with open(p_file_path,'r') as csvfile:
                        csv_reader = csv.DictReader(csvfile)
                        for row in csv_reader:
                            prov_dic[row['oci']] = {'agent': row['agent'],'source': row['source'],'datetime': row['datetime']}

        #iterate all data entries
        for files in os.walk(data_dir):
            for file in files:
                if file.lower().endswith('.csv'):
                    d_file_path = os.path.join(data_dir, file)
                    #open the csv file and process each data entry
                    with open(d_file_path,'r') as csvfile:
                        csv_reader = csv.DictReader(csvfile)
                        for row in csv_reader:
                            # oci , citing , cited , creation , timespan
                            # get the prov data from the prov dictionary oci key
                            # create the rdf and generate the files
                            oci_prov = {'agent': None,'source': None,'datetime': None}
                            if row['oci'] in prov_dic:
                                oci_prov = prov_dic[row['oci']]

                            citation = Citation( None, 'citing_url', None,
                                                None, 'cited_url', None,
                                                oci_prov['agent'], oci_prov['source'],oci_prov['daterime'],
                                                creation_date= row['creation'], duration= row['timespan'], oci= row['oci'])

    print("Done")
