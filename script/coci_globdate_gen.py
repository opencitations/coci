
import os
from argparse import ArgumentParser
from citation import Citation
from rdflib.namespace import RDF, RDFS, SKOS
import csv
from datetime import datetime


def update_dates(date_val, all_dates):
    #check length string
    len_date_val = len(date_val)
    for date_key in all_dates:
        len_alldates = len(date_key)
        break
    # I have a better date already
    if len_date_val < len_alldates:
        return all_dates
    # I have a new better date
    elif len_date_val > len_alldates:
        new_alldates = {}
        new_alldates[date_val] = 1
        return new_alldates
    # New date has equl importance to others
    elif len_date_val == len_alldates:
        all_dates[date_val] = 1
        return all_dates


def getdateformat(date_str):
    str_format = ""
    if Citation.contains_years(date_str):
        str_format = str_format + "%Y"
    if Citation.contains_months(date_str):
        str_format = str_format + "-%m"
    if Citation.contains_days(date_str):
        str_format = str_format + "-%d"
    return str_format


if __name__ == "__main__":
    arg_parser = ArgumentParser("coci_globdate_gen.py", description="Create a global dates index")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the COCI processed data")
    arg_parser.add_argument("-out", "--output", dest="output_file", required=False, help="Path/dir of the output file")
    arg_parser.add_argument("-g", "--input_glob_file", dest="input_glob_file", required=False, help="Path+Name of the global dates index file. In case not specified create the file is empty and is created from zero")
    arg_parser.add_argument("-pindex", dest="pindex", required=False, help="Path+Name of the dirs processed index file")
    args = arg_parser.parse_args()

    INPUT_ROOT_DIR = "."
    PROCESSED_INDEX = "globdateindex/processed.csv"
    OUTPUT_FILE = "globdateindex/globdates.csv"

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    if args.pindex:
        PROCESSED_INDEX = args.pindex

    if args.output_file:
        PROCESSED_INDEX = args.output_file+"/processed.csv"
        OUTPUT_FILE = args.output_file+"/globdates.csv"

    #create output file
    if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILE))
            pass
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    with open(OUTPUT_FILE, 'w') as f:
        f.write("id,value")

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


    #in case not specified create the global dates file is empty and gets created from zero
    glob_date_dic = {}
    if args.input_glob_file:
        GLOB_DATES = args.input_glob_file
        if not os.path.exists(os.path.dirname(GLOB_DATES)):
            print("Path to global dates not correct!")
            raise
        else:
            #init the global dictionary
            with open(GLOB_DATES,'r') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                for row in csv_reader:
                    doi_key = row['id']
                    date_val = row['value']
                    glob_date_dic[doi_key] = {}
                    glob_date_dic[doi_key][date_val] = 1


    all_dirs = [ os.path.join(INPUT_ROOT_DIR, name) for name in os.listdir(INPUT_ROOT_DIR) if os.path.isdir(os.path.join(INPUT_ROOT_DIR, name)) ]

    print("The Root directory is %s"%(INPUT_ROOT_DIR))
    for d in all_dirs:
        print("Processing %s"%(d))
        if d not in processed_dic:
            date_file_path = '%s/index/date.csv'%(d)

            with open(date_file_path,'r') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                for row in csv_reader:
                    doi_key = row['id']
                    date_val = row['value']
                    if doi_key in glob_date_dic:
                        if date_val in glob_date_dic[doi_key]:
                            glob_date_dic[doi_key][date_val] = glob_date_dic[doi_key][date_val] + 1
                        else:
                            print(glob_date_dic[doi_key])
                            print(date_val)
                            glob_date_dic[doi_key] = update_dates(date_val, glob_date_dic[doi_key])
                    else:
                        glob_date_dic[doi_key] = {}
                        glob_date_dic[doi_key][date_val] = 1

        processed_dic[d] = 1
        with open(PROCESSED_INDEX, 'a', newline='') as f:
            f.write(d)
    print("Done")
    #print(glob_date_dic)


    #get best date from the dates list of each doi
    block_txt = ""
    for doikey in glob_date_dic:
        bestdate = []
        maxscore = 0
        for datekey in glob_date_dic[doikey]:
            if glob_date_dic[doikey][datekey] > maxscore:
                maxscore = glob_date_dic[doikey][datekey]
                bestdate = [datekey]
            elif glob_date_dic[doikey][datekey] == maxscore:
                bestdate.append(datekey)
        # choose only 1 date from the list
        # the most recent date
        choosen_date = "1000-01-01"
        for d in bestdate:
            d_datetime = datetime.strptime(d, getdateformat(d))
            choosen_date_datetime = datetime.strptime(choosen_date, getdateformat(choosen_date))
            if d_datetime > choosen_date_datetime:
                choosen_date = d
        #update dictionary with the choosen date
        #glob_date_dic[doikey] = choosen_date
        block_txt = block_txt + '\n"%s",%s'%(doikey.replace('"', '""'),choosen_date)

    with open(OUTPUT_FILE, 'a', newline='') as f:
        f.write(block_txt)
