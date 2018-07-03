import os
from argparse import ArgumentParser
from citation import Citation
import csv
import datetime
from time import sleep
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import citation


def escape_inner_quotes(str_val):
    return str_val.replace('"', '""')

if __name__ == "__main__":
    arg_parser = ArgumentParser("coci_update_date.py", description="Update and create a new csv by giving a new global dates index")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the COCI processed data")
    arg_parser.add_argument("-out", "--output", dest="output_file", required=False, help="Path/dir of the output file")
    arg_parser.add_argument("-g", "--input_glob_file", dest="input_glob_file", required=False, help="Path+Name of the global dates index file")
    arg_parser.add_argument("-pindex", dest="pindex", required=False, help="Path+Name of the dirs processed index file")
    args = arg_parser.parse_args()

    INPUT_ROOT_DIR = "."
    PROCESSED_INDEX = "updated_date/processed.csv"
    OUTPUT_FILE = "updated_date/data.csv"
    OUTPUT_FILE_PROV = "updated_date/prov.csv"
    DATE_GLOB = "date.csv"

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    if args.input_glob_file:
        DATE_GLOB = args.input_glob_file

    if args.output_file:
        PROCESSED_INDEX = args.output_file+"/processed.csv"
        OUTPUT_FILE = args.output_file+"/data.csv"
        OUTPUT_FILE_PROV = args.output_file+"/prov.csv"

    #create DATA output file
    if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILE))
            pass
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    with open(OUTPUT_FILE, 'w') as f:
        f.write('oci,citing,cited,creation,timespan')

    #create PROV output file
    if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILE))
            pass
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    with open(OUTPUT_FILE_PROV, 'w') as f:
        f.write('oci,agent,source,datetime')


    #create processed file
    processed_dic = {}
    if not os.path.exists(PROCESSED_INDEX):
        try:
            if not os.path.exists(os.path.dirname(PROCESSED_INDEX)):
                os.makedirs(os.path.dirname(PROCESSED_INDEX))
            with open(PROCESSED_INDEX, 'w') as f:
                pass
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    else:
        with open(PROCESSED_INDEX,'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            for row in csv_reader:
                processed_dic[row['id']] = 1


    #init the global dictionary
    glob_date_dic = {}
    with open(DATE_GLOB,'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            doi_key = row['id']
            date_val = row['value']
            glob_date_dic[doi_key] = date_val



    #scan all data files
    all_dirs = [ os.path.join(INPUT_ROOT_DIR, name) for name in os.listdir(INPUT_ROOT_DIR) if os.path.isdir(os.path.join(INPUT_ROOT_DIR, name)) ]
    print("The Root directory is %s"%(INPUT_ROOT_DIR))
    for d in all_dirs:
        print("Processing %s  ..."%(d))
        data_file_path = '%s/data'%(d)
        prov_file_path = '%s/prov'%(d)

        for dfile in os.listdir(prov_file_path):
            dfile_path = '%s/%s'%(prov_file_path,dfile)
            if dfile_path not in processed_dic:
                #process it
                block_txt = ""
                with open(dfile_path,'r') as f:
                    lines = f.readlines()
                    for row in lines:
                        block_txt = block_txt + '\n%s'%(row)
                        #break

                with open(OUTPUT_FILE_PROV, 'a', newline='') as f:
                    f.write(block_txt)


        for dfile in os.listdir(data_file_path):
            dfile_path = '%s/%s'%(data_file_path,dfile)
            if dfile_path not in processed_dic:
                #process it
                block_txt = ""
                with open(dfile_path,'r') as csvfile:
                    csv_reader = csv.DictReader(csvfile)
                    for row in csv_reader:
                        #oci,citing,cited,creation,timespan
                        oci = row['oci']
                        citing = row['citing']
                        cited = row['cited']
                        new_creation = row['creation']
                        timespan = row['timespan']
                        try:
                            new_creation = glob_date_dic[citing]
                            new_cited_time = glob_date_dic[cited]

                            default_date = datetime.datetime(1970, 1, 1, 0, 0)
                            try:
                                citing_dt = parse(new_creation, default=default_date)
                                cited_dt = parse(new_cited_time, default=default_date)
                                delta = relativedelta(citing_dt, cited_dt)
                                timespan = citation.Citation.get_duration(delta,
                                                                  citation.Citation.contains_months(new_creation) and citation.Citation.contains_months(new_cited_time),
                                                                  citation.Citation.contains_days(new_creation) and citation.Citation.contains_days(new_cited_time))

                                block_txt = block_txt +'\n%s,"%s","%s",%s,%s'%(oci,escape_inner_quotes(citing),escape_inner_quotes(cited),new_creation,timespan)

                            except:
                                pass
                        except:
                            block_txt = block_txt +'\n%s,"%s","%s",%s,%s'%(oci,escape_inner_quotes(citing),escape_inner_quotes(cited),new_creation,timespan)

                with open(OUTPUT_FILE, 'a', newline='') as f:
                    f.write(block_txt)
