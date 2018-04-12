
import os
from argparse import ArgumentParser
from citation import Citation
from rdflib.namespace import RDF, RDFS, SKOS
import csv


if __name__ == "__main__":
    arg_parser = ArgumentParser("coci_globdate_gen.py", description="Create a global dates index")
    arg_parser.add_argument("-in", "--input_dir", dest="input_dir", required=True, help="The root directory of the COCI processed data")
    arg_parser.add_argument("-g", "--input_glob_file", dest="input_glob_file", required=False, help="Path+Name of the global dates index file. In case not specified create the file is empty and is created from zero")
    arg_parser.add_argument("-pindex", dest="pindex", required=False, help="Path+Name of the dirs processed index file")
    args = arg_parser.parse_args()

    INPUT_ROOT_DIR = "."
    OUTPUT_FILE = "globdateindex/globdates.nt"
    PROCESSED_INDEX = "globdateindex/processed.csv"

    if args.input_dir:
        INPUT_ROOT_DIR = args.input_dir

    if args.pindex:
        PROCESSED_INDEX = args.pindex

    processed_dic = {}
    if not os.path.exists(os.path.dirname(PROCESSED_INDEX)):
        try:
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

    print(processed_dic)

    #in case not specified create the global dates file is empty and gets created from zero
    glob_date_dic = {}
    if args.input_glob_file:
        OUTPUT_FILE = args.input_glob_file
        if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
            try:
                os.makedirs(os.path.dirname(OUTPUT_FILE))
                with open(OUTPUT_FILE, 'w') as f:
                    pass
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        else:
            globdate_file_path = OUTPUT_FILE
            #init the global dictionary
            with open(globdate_file_path,'r') as csvfile:
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
                            glob_date_dic[doi_key] = update_dates(date_val, glob_date_dic[doi_key])
                            #check my date compared with other dates
                    else:
                        glob_date_dic[doi_key] = {}
                        glob_date_dic[doi_key][date_val] = 1

        processed_dic[d] = 1
        with open(PROCESSED_INDEX, 'a', newline='') as f:
            f.write(d)
    print("Done")
    print(glob_date_dic)

def update_dates(date_val, all_dates):

    if date_val in all_dates:
        all_dates[date_val] = all_dates[date_val] + 1
        return all_dates

    all_dates_years = []
    all_dates_months = []
    all_dates_days = []
    for date_key in all_dates:
        i_date = get_date_parts(date_key)
        all_dates_years.append(i_date.year)
        all_dates_months.append(i_date.month)
        all_dates_days.append(i_date.day)

    my_date = get_date_parts(date_val)

    yearindex = all_dates_years.index(my_date.year)
    if yearindex != -1:
        com_res = best_date(my_date.month, all_dates_months[yearindex])
        if com_res == 1:
            all_dates.pop(rebuild_date_parts(all_dates_years[yearindex], all_dates_months[yearindex], all_dates_days[yearindex]), None)
            all_dates[date_val] = 1
        elif com_res == 2:
            pass
        elif com_res == 0:
            com_res = best_date(my_date.day, all_dates_days[yearindex])
            if com_res == 1:
                all_dates.pop(rebuild_date_parts(all_dates_years[yearindex], all_dates_months[yearindex], all_dates_days[yearindex]), None)
                all_dates[date_val] = 1
            elif com_res == 2:
                pass
            elif com_res == 0:
                all_dates[date_val] = all_dates[date_val] + 1

    return all_dates

    def best_date(date_a, date_b):
        if date_a == date_b:
            return 0
        elif date_a == None and date_b != None :
            return 2
        else:
            return 1

def rebuild_date_parts(year,month,day):
    str_date = ""
    if year != None:
        str_date = year
        if month != None:
            str_date = str_date + "-" + month
            if day != None:
                str_date = str_date + "-" + day
    return str_date


def get_date_parts(date_val):
    year = None
    if Citation.contains_years(date_val):
        year = date_val[0:4]

    month = None
    if Citation.contains_months(date_val):
        month = date_val[6:7]

    day = None
    if Citation.contains_days(date_val):
        day = date_val[9:10]

    return {"year": year, "month": month, "day":day}
